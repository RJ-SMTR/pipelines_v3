{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

with
    gps as (
        select data, id_veiculo
        from {{ ref("view_gps_sppo_completo") }}
        where data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")

        union all

        select data, id_veiculo
        from {{ ref("view_gps_onibus") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select data, id_veiculo, count(*) as quantidade_gps
from gps
where
    data > '{{ var("data_final_veiculo_arquitetura_1") }}'
    {% if is_incremental() %}
        and data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}
group by 1, 2
