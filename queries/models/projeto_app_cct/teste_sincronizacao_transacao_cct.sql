{{
    config(
        materialized="table",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set transacao_cct = ref("transacao_cct") %}
{% set source_teste_sincronizacao_transacao_cct = source(
    "source_cct", "teste_sincronizacao_transacao_cct"
) %}
{% set relation = adapter.get_relation(
    database=transacao_cct.database,
    schema=transacao_cct.schema,
    identifier=transacao_cct.identifier,
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
                concat("'", data, "'") as particao
            from
                {{ source_teste_sincronizacao_transacao_cct }}
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

{% set sha_column %}
    sha256(
        concat(
            {% for c in column_names %}
                ifnull(
                    cast(
                        {% if c == "datetime_transacao" %}
                            datetime({{ c }})
                        {% elif c == "valor_pagamento" %}
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
        from {{ source_teste_sincronizacao_transacao_cct }}
        qualify
            datetime_extracao_teste
            = max(datetime_extracao_teste) over (partition by id_transacao)
    ),
    postgres as (
        select *, {{ sha_column }} as sha_dados_postgres from postgres_deduplicado
    ),
    bq as (
        select *, {{ sha_column }} as sha_dados_bigquery
        from {{ transacao_cct }}
        where data in ({{ partitions | join(", ") }})
    ),
    dados_novos as (
        select
            ifnull(b.data, p.data) as data,
            b.data as data_bigquery,
            p.data as data_postgres,
            id_transacao,
            sha_dados_bigquery,
            sha_dados_postgres,
        from bq b
        full outer join postgres p using (id_transacao)
    ),
    dados_completos as (
        select *
        from dados_novos
        {% if table_exists(this) %}
            union all

            select * except (versao, datetime_ultima_atualizacao, id_execucao_dbt)
            from {{ this }}
            where id_transacao not in (select id_transacao from dados_novos)
        {% endif %}
    )

select
    *,
    '{{ var("version") }}' as versao,
    current_datetime('America/Sao_Paulo') as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from dados_completos
where ifnull(to_hex(sha_dados_bigquery), '') != ifnull(to_hex(sha_dados_postgres), '')
