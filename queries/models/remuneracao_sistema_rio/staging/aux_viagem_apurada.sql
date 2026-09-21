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

with
    openfisca as (
        select *
        from {{ ref("aux_viagem_apurada_openfisca") }}
        where {{ incremental_filter }}
    ),
    viagem as (
        select data, id_viagem, tecnologia_minima_servico
        from {{ ref("viagem_valida_classificada") }}
        where {{ incremental_filter }}
    )
select
    o.id_apuracao,
    o.id_viagem,
    cast(o.datetime_partida as datetime) as datetime_partida,
    cast(o.datetime_chegada as datetime) as datetime_chegada,
    o.indicador_viagem_completa,
    o.indicador_viagem_valida,
    o.indicador_viagem_conforme,
    cast(null as bool) as indicador_dentro_do_teto_programado,
    o.km_programada,
    o.km_percorrida,
    o.lote,
    cast(o.lote_frota_estimada as int64) as lote_frota_estimada,
    o.id_veiculo,
    o.placa,
    o.servico,
    o.sentido,
    o.shape_id,
    cast(o.faixa_horaria_inicio as datetime) as faixa_horaria_inicio,
    cast(o.faixa_horaria_fim as datetime) as faixa_horaria_fim,
    o.tecnologia_minima_servico,
    cast(o.lote_frota_determinada as int64) as lote_frota_determinada,
    o.lote_km_referencia_mensal,
    o.lote_km_referencia as lote_km_referencia_quinzena,
    o.tipo_dia,
    extract(hour from o.datetime_partida) as hora_partida,
    o.data,
    o.indicador_km_pagamento as indicador_quilometragem_pagamento,
    o.indicador_percentual_atendimento,
    o.km_remuneravel,
    o.indicador_pico_manha,
    o.indicador_pico_tarde,
    o.indicador_viagem_completa
    and o.indicador_pico_manha as indicador_completa_pico_manha,
    o.indicador_viagem_completa
    and o.indicador_pico_tarde as indicador_completa_pico_tarde,
    o.indicador_dia_util,
    o.tarifa_remuneracao,
    o.alpha,
    o.beta,
    cast(o.frota_pico_manha as int64) as frota_pico_manha,
    cast(o.frota_pico_tarde as int64) as frota_pico_tarde,
    cast(o.frota_operante as int64) as frota_operante,
    o.viagens_atendimento_faixa,
    o.viagens_programadas_faixa,
    o.percentual_atendimento,
    o.ipa,
    o.desconto_operacao_precaria,
    o.km_remuneravel_faixa,
    o.km_ponderada_ipa_faixa,
    o.km_ponderada_ipa_viagem,
    o.remuneracao_opex_viagem,
    o.versao_regra,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from openfisca as o
left join viagem as v on v.data = o.data and v.id_viagem = o.id_viagem
