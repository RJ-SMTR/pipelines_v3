{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["id_tipo_veiculo"],
    )
}}

select *
from {{ ref("staging_stu_tipo_de_veiculo") }}
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
qualify row_number() over (partition by id_tipo_veiculo order by data desc) = 1
