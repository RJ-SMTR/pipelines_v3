{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["ratr"],
    )
}}

select *
from {{ ref("staging_stu_pessoa_fisica") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
qualify row_number() over (partition by ratr order by data desc) = 1
