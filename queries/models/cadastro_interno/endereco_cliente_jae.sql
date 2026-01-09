{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        partition_by={
            "field": "id_cliente_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 1000000000, "interval": 100000},
        },
        unique_key="id_cliente_sequencia",
    )
}}

{% set staging_endereco = ref("staging_endereco") %}


{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
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
                    ifnull(cast({{ c }} as string), 'n/a')
                    {% if not loop.last %}, {% endif %}

                {% endfor %}
            )
        )
    {% endset %}
    {% set partitions_query %}
        with
            ids as (
                select distinct cast(cd_cliente as integer) as id
                from {{ staging_endereco }}
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
                "id_cliente_particao between ",
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
    tipo_endereco as (
        select chave as cd_tipo_endereco, valor as tipo_endereco
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "endereco" and coluna = "cd_tipo_endereco"
    ),
    dados_novos as (
        select
            cast(e.cd_cliente as integer) as id_cliente_particao,
            concat(e.cd_cliente, "-", e.nr_seq_endereco) as id_cliente_sequencia,
            e.cd_cliente as id_cliente,
            e.nr_seq_endereco as numero_sequencia_endereco,
            te.tipo_endereco,
            e.nr_cep as cep,
            e.tx_logradouro as logradouro,
            nullif(trim(e.nr_logradouro), '') as numero,
            e.tx_complemento_logradouro as complemento,
            e.nm_bairro as bairro,
            e.nm_cidade as cidade,
            e.sg_uf as uf,
            e.dt_inclusao as datetime_inclusao,
            e.dt_inativacao as datetime_inativacao,
            timestamp_captura as datetime_captura
        from {{ staging_endereco }} e
        left join tipo_endereco te using (cd_tipo_endereco)
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_cliente, numero_sequencia_endereco
                order by datetime_captura desc
            )
            = 1
    ),
    sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from dados_novos),
    sha_dados_atuais as (
        {% if is_incremental() and partitions | length > 0 %}

            select
                id_cliente_sequencia,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where {{ partitions | join("\nor ") }}

        {% else %}
            select
                cast(null as string) as id_cliente_sequencia,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_cliente_sequencia)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_cliente_sequencia)
    ),
    cliente_colunas_controle as (
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
    )
select *
from cliente_colunas_controle
