{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        on_schema_change="append_new_columns",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Tabela larga da apuração (Tabelas Remuneração Sistema RIO, 2026-09-14).
  Fecha o contrato de 52 colunas somando à saída do OpenFisca o que ele não
  calcula: `shape_id` (fan-in), stubs de lote (`servico_oferta_faixa`) e
  `id_apuracao`.

  Origem de `viagem_apurada`, `faixa_apurada` e `data_apurada` — o corte de
  cada uma segue a coluna `granularity` da planilha.

  Stubs: `indicador_dentro_do_teto_programado` (o OF não ranqueia teto) e
  `servico_tipo` (sem fonte no projeto).
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    openfisca as (
        select *
        from {{ ref("aux_viagem_apurada_openfisca") }}
        where {{ incremental_filter }}
    ),
    viagem as (  -- `shape_id` não é cálculo do OF, volta do fan-in pela viagem
        select data, id_viagem, shape_id
        from {{ ref("viagem_valida_classificada") }}
        where {{ incremental_filter }}
    ),
    oferta as (  -- Stubs de lote; nulos até a fonte I.2 entrar
        select distinct
            data,
            lote,
            lote_frota_estimada,
            lote_frota_determinada,
            lote_qr_mensal,
            lote_km_referencia
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }} and lote is not null
    )
select
    concat(
        format_date('%Y-%m-%d', o.data),
        '|',
        o.lote,
        '|',
        o.id_veiculo,
        '|',
        format_datetime('%Y-%m-%dT%H:%M:%S', o.datetime_partida)
    ) as id_apuracao,
    o.id_viagem,
    o.datetime_partida,
    o.datetime_chegada,
    o.indicador_viagem_completa,
    o.indicador_viagem_valida,
    o.indicador_viagem_conforme,
    cast(null as bool) as indicador_dentro_do_teto_programado,
    o.km_programada,
    o.km_percorrida,
    o.lote,
    cast(l.lote_frota_estimada as int64) as lote_frota_estimada,
    o.id_veiculo,
    o.placa,
    o.servico,
    o.sentido,
    v.shape_id,
    o.faixa_horaria_inicio,
    o.faixa_horaria_fim,
    cast(null as string) as servico_tipo,
    o.tecnologia_minima_servico,
    cast(l.lote_frota_determinada as int64) as lote_frota_determinada,
    l.lote_qr_mensal,
    l.lote_km_referencia,
    o.tipo_dia,
    o.hora_partida,
    o.day_of_week,
    o.data,
    o.indicador_quilometragem_pagamento,
    o.indicador_percentual_atendimento,
    o.km_remuneravel,
    o.indicador_pico_manha,
    o.indicador_pico_tarde,
    o.indicador_completa_pico_manha,
    o.indicador_completa_pico_tarde,
    o.indicador_dia_util,
    o.tarifa_remuneracao,
    o.alpha,
    o.beta,
    o.period,
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
    o.tecnologia_fcf,
    o.versao_regra,
    o.id_execucao,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from openfisca as o
left join viagem as v on v.data = o.data and v.id_viagem = o.id_viagem
left join oferta as l on l.data = o.data and l.lote = o.lote
