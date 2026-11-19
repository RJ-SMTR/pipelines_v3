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

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

select
    data,
    lote,
    any_value(lote_frota_estimada) as lote_frota_estimada,
    any_value(lote_frota_determinada) as lote_frota_determinada,
    any_value(lote_km_referencia_mensal) as lote_km_referencia_mensal,
    any_value(lote_km_referencia_quinzena) as lote_km_referencia_quinzena,
    any_value(tipo_dia) as tipo_dia,
    any_value(tarifa_remuneracao) as tarifa_remuneracao,
    any_value(alpha) as alpha,
    any_value(beta) as beta,
    any_value(frota_pico_manha) as frota_pico_manha,
    any_value(frota_pico_tarde) as frota_pico_tarde,
    any_value(frota_operante) as frota_operante,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("aux_viagem_apurada") }}
where {{ incremental_filter }}
group by data, lote
