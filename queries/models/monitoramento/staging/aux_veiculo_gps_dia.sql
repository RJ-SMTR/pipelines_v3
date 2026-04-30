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

select data, id_veiculo, count(*) as quantidade_gps
from {{ ref("gps_sppo") }}
{# from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` #}
where
    data > '{{ var("data_final_veiculo_arquitetura_1") }}'
    {% if is_incremental() %}
        and data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endif %}
group by 1, 2
