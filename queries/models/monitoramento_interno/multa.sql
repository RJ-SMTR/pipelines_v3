{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["serie", "cm"],
    )
}}

select *
from {{ ref("staging_stu_multa") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
qualify row_number() over (partition by serie, cm order by data desc) = 1
