{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["placa", "tptran", "tpperm", "termo"],
    )
}}

select *
from {{ ref("staging_stu_vistoria") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
qualify
    row_number() over (
        partition by placa, tptran, tpperm, termo order by data_vistoria desc
    )
    = 1
