{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["darm"],
    )
}}

select *
from {{ ref("staging_stu_darm_apropriacao") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
qualify row_number() over (partition by darm order by data desc) = 1
