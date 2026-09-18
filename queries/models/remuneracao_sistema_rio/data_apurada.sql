{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Recorte de dia de `aux_viagem_apurada` — colunas de `granularity` "dia"
  na planilha Tabelas Remuneração Sistema RIO (2026-09-14).
  Grão: data × lote. Alimenta o CAPEX e o FCF do sumário de quinzena.

  Fora do recorte, por quebrarem o grão: `id_apuracao` (chave de viagem) e
  `tecnologia_minima_servico` (varia por serviço dentro do lote — o FCF
  tipológico lê a tecnologia direto de `aux_viagem_apurada`).
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

select distinct
    lote,
    lote_frota_estimada,
    lote_frota_determinada,
    lote_qr_mensal,
    lote_km_referencia,
    tipo_dia,
    data,
    indicador_dia_util,
    tarifa_remuneracao,
    alpha,
    beta,
    period,
    frota_pico_manha,
    frota_pico_tarde,
    frota_operante,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("aux_viagem_apurada") }}
where {{ incremental_filter }}
