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
    max(lote_frota_estimada) as lote_frota_estimada,
    max(lote_frota_determinada) as lote_frota_determinada,
    max(lote_km_referencia_mensal) as lote_km_referencia_mensal,
    max(lote_km_referencia_quinzena) as lote_km_referencia_quinzena,
    any_value(tipo_dia) as tipo_dia,
    logical_or(indicador_dia_util) as indicador_dia_util,
    max(tarifa_remuneracao) as tarifa_remuneracao,
    max(alpha) as alpha,
    max(beta) as beta,
    max(frota_pico_manha) as frota_pico_manha,
    max(frota_pico_tarde) as frota_pico_tarde,
    max(frota_operante) as frota_operante,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("aux_viagem_apurada") }}
where {{ incremental_filter }}
group by data, lote
