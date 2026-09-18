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
  Recorte de viagem de `aux_viagem_apurada` — colunas de `granularity`
  "viagem" na planilha Tabelas Remuneração Sistema RIO (2026-09-14).
  Grão: uma linha por `id_apuracao`.
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

select
    id_apuracao,
    id_viagem,
    datetime_partida,
    datetime_chegada,
    indicador_viagem_completa,
    indicador_viagem_valida,
    indicador_viagem_conforme,
    km_programada,
    km_percorrida,
    lote,
    id_veiculo,
    placa,
    servico,
    sentido,
    shape_id,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    tipo_dia,
    data,
    indicador_quilometragem_pagamento,
    indicador_percentual_atendimento,
    km_remuneravel,
    indicador_pico_manha,
    indicador_pico_tarde,
    km_ponderada_ipa_viagem,
    remuneracao_opex_viagem,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("aux_viagem_apurada") }}
where {{ incremental_filter }}
