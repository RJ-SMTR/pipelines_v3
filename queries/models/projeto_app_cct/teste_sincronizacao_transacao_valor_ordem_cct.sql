{{
    config(
        materialized="table",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set transacao_valor_ordem_cct = ref("transacao_valor_ordem_cct") %}
{% set source_teste_sincronizacao = source(
    "source_cct", "teste_sincronizacao_transacao_valor_ordem_cct"
) %}
{% set relation = adapter.get_relation(
    database=transacao_valor_ordem_cct.database,
    schema=transacao_valor_ordem_cct.schema,
    identifier=transacao_valor_ordem_cct.identifier,
) %}
{% set column_names = (
    adapter.get_columns_in_relation(relation)
    | map(attribute="name")
    | reject(
        "equalto",
        "datetime_ultima_atualizacao",
    )
    | list
) %}

{% if execute %}
    {% set partitions_query %}
            select distinct
                concat("'", data_ordem, "'") as particao
            from
                {{ source_teste_sincronizacao }}
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

{% set sha_column %}
    sha256(
        concat(
            {% for c in column_names %}
                ifnull(
                    cast(
                        {% if c == "valor_transacao_rateio" %}
                            round({{ c }}, 5)
                        {% else %}
                            {{ c }}
                        {% endif %}
                        as string
                    ),
                    'N/A'
                )
                {% if not loop.last %},{% endif %}

            {% endfor %}
        )
    )
{% endset %}

with
    postgres_deduplicado as (
        select *
        from {{ source_teste_sincronizacao }}
        qualify
            datetime_extracao_teste
            = max(datetime_extracao_teste) over (partition by id_transacao, data_ordem)
    ),
    postgres as (
        select *, {{ sha_column }} as sha_dados_postgres from postgres_deduplicado
    ),
    bq as (
        select *, {{ sha_column }} as sha_dados_bigquery
        from {{ transacao_valor_ordem_cct }}
        where data_ordem in ({{ partitions | join(", ") }})
    ),
    dados_novos as (
        select
            ifnull(b.data_ordem, p.data_ordem) as data_ordem,
            b.data_ordem as data_ordem_bigquery,
            p.data_ordem as data_ordem_postgres,
            b.id_transacao,
            sha_dados_bigquery,
            sha_dados_postgres,
        from bq b
        full outer join postgres p using (id_transacao, data_ordem)
    ),
    dados_completos as (
        select *
        from dados_novos
        {% if table_exists(this) %}
            union all

            select * except (versao, datetime_ultima_atualizacao, id_execucao_dbt)
            from {{ this }}
            where
                (id_transacao, data_ordem)
                not in (select id_transacao, data_ordem from dados_novos)
        {% endif %}
    )

select
    *,
    '{{ var("version") }}' as versao,
    current_datetime('America/Sao_Paulo') as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from dados_completos
where ifnull(to_hex(sha_dados_bigquery), '') != ifnull(to_hex(sha_dados_postgres), '')
