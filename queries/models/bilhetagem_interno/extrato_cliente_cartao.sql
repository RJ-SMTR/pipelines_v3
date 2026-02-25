{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        require_partition_filter=true,
    )
}}


{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% set staging_lancamento = ref("staging_lancamento") %}

{% if is_incremental() %}
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

    {% set lancamento_partitions_query %}
        select distinct concat("'", date(dt_lancamento), "'") as dt_lancamento
        from {{ staging_lancamento }}
        where {{ incremental_filter }}
    {% endset %}

    {% set lancamento_partitions = (
        run_query(lancamento_partitions_query).columns[0].values()
    ) %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    dados_novos as (
        select
            date(l.dt_lancamento) as data,
            l.dt_lancamento as datetime_lancamento,
            cast(l.id_conta as string) as id_conta,
            case
                when regexp_contains(l.id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
                then split(l.id_conta, ".")[offset(3)]
            end as hash_cartao,
            ifnull(
                l.id_lancamento, concat(string(l.dt_lancamento), '_', l.id_movimento)
            ) as id_unico_lancamento,
            l.id_lancamento,
            l.cd_cliente as id_cliente,
            j.nome as nome_cliente,
            j.documento as documento_cliente,
            j.tipo_documento as tipo_documento_cliente,
            l.ds_tipo_movimento as tipo_movimento,
            l.vl_lancamento as valor_lancamento,
            l.timestamp_captura as datetime_captura
        from {{ ref("staging_lancamento") }} as l
        left join {{ ref("cliente_jae") }} j on j.id_cliente = l.cd_cliente
        where
            (
                regexp_contains(l.id_conta, r'^2\.2\.1\.[A-Za-z0-9]+\.(1|2|6)$')
                or regexp_contains(l.id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
            )
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    dados_novos_deduplicado as (
        select *
        from dados_novos
        qualify
            row_number() over (
                partition by id_unico_lancamento, id_conta
                order by datetime_captura desc
            )
            = 1
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if lancamento_partitions | length > 0 %}
                    data in ({{ lancamento_partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}

        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from dados_novos_deduplicado

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais

        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify
            row_number() over (
                partition by id_unico_lancamento, id_conta
                order by priority, datetime_lancamento desc
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_unico_lancamento,
                id_conta,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_unico_lancamento,
                cast(null as string) as id_conta,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_unico_lancamento, id_conta)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_unico_lancamento, id_conta)
    ),
    extrato_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
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
from extrato_colunas_controle
