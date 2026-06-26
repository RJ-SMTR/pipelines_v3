{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        alias="licenciamento_stu",
    )
}}

select * replace(date(data) as data)
from {{ ref("staging_licenciamento_stu_v1") }}
where
    date(data) <= date("2026-06-23")
    {% if is_incremental() %}
        and date(data) between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}

union all by name

select *
from {{ ref("staging_licenciamento_stu_v2") }}
where
    data > "2026-06-23"
    {% if is_incremental() %}
        and data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}
