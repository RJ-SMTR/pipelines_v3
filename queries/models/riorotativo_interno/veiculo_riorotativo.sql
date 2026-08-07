{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        partition_by={
            "field": "id_veiculo_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 1000000000, "interval": 100000},
        },
        unique_key="id_veiculo",
        alias="veiculo",
    )
}}

{% set staging_veiculo_riorotativo = ref("staging_veiculo_riorotativo") %}
{% set staging_veiculo_cliente_riorotativo = ref(
    "staging_veiculo_cliente_riorotativo"
) %}


{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

-- busca quais partições serão atualizadas pelas capturas
{% if execute and is_incremental() %}
    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            concat(
                {% for c in columns %}
                    {% if c == "motoristas" %}
                        ifnull(to_json_string(motoristas), 'n/a')
                    {% else %}ifnull(cast({{ c }} as string), 'n/a')
                    {% endif %}

                    {% if not loop.last %}, {% endif %}

                {% endfor %}
            )
        )
    {% endset %}
    {% set partitions_query %}
        with
            ids as (
                select distinct cast(id_veiculo as integer) as id
                from {{ staging_veiculo_riorotativo }}
                where {{ incremental_filter }}

                union distinct

                select distinct cast(id_veiculo as integer) as id
                from {{ staging_veiculo_cliente_riorotativo }}
                where {{ incremental_filter }}
            ),
            grupos as (select distinct div(id, 100000) as group_id from ids),
            identifica_grupos_continuos as (
                select
                    group_id,
                    if(
                        lag(group_id) over (order by group_id) = group_id - 1, 0, 1
                    ) as id_continuidade
                from grupos
            ),
            grupos_continuos as (
                select
                    group_id, sum(id_continuidade) over (order by group_id) as id_continuidade
                from identifica_grupos_continuos
            )
        select
            distinct
            concat(
                "id_veiculo_particao between ",
                min(group_id) over (partition by id_continuidade) * 100000,
                " and ",
                (max(group_id) over (partition by id_continuidade) + 1) * 100000 - 1
            )
        from grupos_continuos
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    veiculo_staging as (
        select
            cast(id_veiculo as integer) as id_veiculo_particao,
            id_veiculo,
            placa,
            id_modelo_veiculo,
            cor,
            datetime_inclusao,
            datetime_captura
        from {{ staging_veiculo_riorotativo }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    veiculo_cliente_staging as (
        select
            id_veiculo,
            array_agg(
                struct(
                    id_veiculo_cliente as id_veiculo_cliente,
                    cpf_motorista as cpf_motorista,
                    datetime_inativacao as datetime_inativacao,
                    datetime_inclusao as datetime_inclusao
                )
            ) as motoristas
        from {{ staging_veiculo_cliente_riorotativo }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        group by 1
    ),
    {% if is_incremental() and partitions | length > 0 %}
        dados_atuais as (
            select * from {{ this }} where {{ partitions | join("\nor ") }}
        ),
        dados_completos as (
            select
                ifnull(
                    s.id_veiculo_particao, a.id_veiculo_particao
                ) as id_veiculo_particao,
                ifnull(s.id_veiculo, a.id_veiculo) as id_veiculo,
                ifnull(s.placa, a.placa) as placa,
                ifnull(s.id_modelo_veiculo, a.id_modelo_veiculo) as id_modelo_veiculo,
                ifnull(s.cor, a.cor) as cor,
                ifnull(
                    a.motoristas,
                    cast(
                        [] as array<
                            struct<
                                id_veiculo_cliente string,
                                cpf_motorista string,
                                datetime_inativacao datetime,
                                datetime_inclusao datetime
                            >
                        >
                    )
                ) as motoristas,
                ifnull(s.datetime_inclusao, a.datetime_inclusao) as datetime_inclusao,
                ifnull(s.datetime_captura, a.datetime_captura) as datetime_captura,
            from veiculo_staging s
            full outer join dados_atuais a using (id_veiculo)
        ),
    {% else %}
        dados_completos as (
            select
                *,
                cast(
                    [] as array<
                        struct<
                            id_veiculo_cliente string,
                            cpf_motorista string,
                            datetime_inativacao datetime,
                            datetime_inclusao datetime
                        >
                    >
                ) as motoristas
            from veiculo_staging
        ),
    {% endif %}
    veiculo_motorista as (
        select
            d.id_veiculo_particao,
            d.id_veiculo,
            d.placa,
            d.id_modelo_veiculo,
            d.cor,
            array(
                select as struct *
                from
                    unnest(
                        array_concat(
                            d.motoristas,
                            ifnull(
                                vc.motoristas,
                                cast(
                                    [] as array<
                                        struct<
                                            id_veiculo_cliente string,
                                            cpf_motorista string,
                                            datetime_inativacao datetime,
                                            datetime_inclusao datetime
                                        >
                                    >
                                )
                            )
                        )
                    )
                qualify
                    row_number() over (
                        partition by id_veiculo_cliente
                        order by datetime_inclusao desc, datetime_inativacao desc
                    )
                    = 1
            ) as motoristas,
            d.datetime_inclusao,
            d.datetime_captura
        from dados_completos d
        left join veiculo_cliente_staging vc using (id_veiculo)
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from veiculo_motorista
    ),
    sha_dados_atuais as (
        {% if is_incremental() and partitions | length > 0 %}

            select
                id_veiculo,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where {{ partitions | join("\nor ") }}

        {% else %}
            select
                cast(null as string) as id_veiculo,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_veiculo)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_veiculo)
    ),
    veiculo_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual
            ),
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
        qualify
            row_number() over (partition by id_veiculo order by datetime_captura desc)
            = 1
    )
select *
from veiculo_colunas_controle
