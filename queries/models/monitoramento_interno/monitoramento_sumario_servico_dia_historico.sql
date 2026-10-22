{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="sumario_servico_dia_historico",
        labels={"dashboard": "yes"},
    )
}}
{% if var("start_date") < var("DATA_SUBSIDIO_V9_INICIO") %}
    select *
    from {{ ref("monitoramento_servico_dia") }}
    where data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
    union all
{% endif %}
select *
from {{ ref("monitoramento_servico_dia_v2") }}
where data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
