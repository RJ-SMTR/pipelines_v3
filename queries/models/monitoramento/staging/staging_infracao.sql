{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        alias="infracao",
    )
}}

select * replace(date(data) as data)
from {{ ref("staging_infracao_v1") }}
where
    date(data) <= date("{{ var('data_inicio_dbstu') }}")
    {% if is_incremental() %}
        and date(data) between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}
    {% if is_incremental() %}
        and date(data) between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}

union all by name

select * except (versao, datetime_ultima_atualizacao, id_execucao_dbt)
from {{ ref("staging_infracao_v2") }}
where
    data > "{{ var('data_inicio_dbstu') }}"
    {% if is_incremental() %}
        and data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}
