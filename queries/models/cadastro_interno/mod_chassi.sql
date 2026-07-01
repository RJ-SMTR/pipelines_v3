{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["id_modelo_chassi"],
    )
}}

select *
from {{ ref("staging_stu_mod_chassi") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
qualify row_number() over (partition by id_modelo_chassi order by data desc) = 1
