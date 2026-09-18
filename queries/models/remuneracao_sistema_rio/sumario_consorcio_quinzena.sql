{{
    config(
        materialized="table",
        partition_by={
            "field": "data_inicio_quinzena",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Fechamento por consórcio × quinzena (planilha Tabelas Remuneração
  Sistema RIO, 2026-09-14).

  OPEX e desconto somam `sumario_servico_sentido_dia`. FCF e CAPEX saem de
  `data_apurada`. Não usa `fcf_quinzena_lote` / `remuneracao_quinzena_lote`,
  que seguem com `enabled=false`.

  CAPEX sai zerado enquanto `lote_frota_estimada` e `lote_km_referencia`
  forem stubs nulos em `servico_oferta_faixa` (fonte I.2 não ligada): sem
  frota estimada não há denominador de FCF.

  Receita da tarifa pública: `bilhetagem_servico_operador_dia` restrita aos
  serviços do lote. Essa tabela já traz o rateio de integração somado ao
  total, então não precisa do tratamento que `transacao_valor_ordem` faz na
  mão. Usa o valor BRUTO (antes da taxa de administração) — trocar para
  `valor_total_transacao_liquido` se a taxa tiver de ser abatida.

  Quinzena: 1 = dias 1–15; 2 = dia 16 ao fim do mês.
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    dia as (
        select
            if(
                extract(day from data) <= 15,
                date_trunc(data, month),
                date_add(date_trunc(data, month), interval 15 day)
            ) as data_inicio_quinzena,
            if(
                extract(day from data) <= 15,
                date_add(date_trunc(data, month), interval 14 day),
                last_day(data, month)
            ) as data_fim_quinzena,
            lote,
            consorcio,
            desconto_operacao_precaria_dia,
            remuneracao_opex_dia
        from {{ ref("sumario_servico_sentido_dia") }}
        where {{ incremental_filter }}
    ),
    opex_quinzena as (
        select
            data_inicio_quinzena,
            data_fim_quinzena,
            lote,
            consorcio,
            sum(desconto_operacao_precaria_dia) as desconto_operacao_precaria_quinzena,
            sum(remuneracao_opex_dia) as remuneracao_opex_quinzena
        from dia
        group by data_inicio_quinzena, data_fim_quinzena, lote, consorcio
    ),
    dia_lote as (
        select
            if(
                extract(day from data) <= 15,
                date_trunc(data, month),
                date_add(date_trunc(data, month), interval 15 day)
            ) as data_inicio_quinzena,
            lote,
            indicador_dia_util,
            frota_operante,
            lote_frota_estimada,
            lote_km_referencia,
            tarifa_remuneracao,
            alpha
        from {{ ref("data_apurada") }}
        where {{ incremental_filter }}
    ),
    frota_quinzena as (
        select
            data_inicio_quinzena,
            lote,
            avg(if(indicador_dia_util, frota_operante, null)) as frota_operante_media,
            max(lote_frota_estimada) as lote_frota_estimada,
            max(lote_km_referencia) as lote_km_referencia,
            max(tarifa_remuneracao) as tarifa_remuneracao,
            max(alpha) as alpha
        from dia_lote
        group by data_inicio_quinzena, lote
    ),
    servico_lote as (  -- Serviços do lote, para recortar a receita da Jaé
        select distinct lote, servico
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }} and lote is not null
    ),
    receita_quinzena as (
        select
            if(
                extract(day from b.data_ordem) <= 15,
                date_trunc(b.data_ordem, month),
                date_add(date_trunc(b.data_ordem, month), interval 15 day)
            ) as data_inicio_quinzena,
            s.lote,
            b.consorcio,
            sum(b.valor_total_transacao_bruto) as receita_tarifa_publica_quinzena
        from {{ ref("bilhetagem_servico_operador_dia") }} as b
        inner join servico_lote as s on s.servico = b.servico_jae
        where
            b.data_ordem between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
        group by data_inicio_quinzena, s.lote, b.consorcio
    ),
    base as (
        select
            o.data_inicio_quinzena,
            o.data_fim_quinzena,
            o.lote,
            o.consorcio,
            o.desconto_operacao_precaria_quinzena,
            o.remuneracao_opex_quinzena,
            least(
                1.0,
                coalesce(
                    safe_divide(f.frota_operante_media, f.lote_frota_estimada), 0.0
                )
            ) as fator_cumprimento_frota,
            f.tarifa_remuneracao,
            f.alpha,
            f.lote_km_referencia,
            coalesce(
                r.receita_tarifa_publica_quinzena, 0.0
            ) as receita_tarifa_publica_quinzena
        from opex_quinzena as o
        left join
            frota_quinzena as f
            on f.data_inicio_quinzena = o.data_inicio_quinzena
            and f.lote = o.lote
        left join
            receita_quinzena as r
            on r.data_inicio_quinzena = o.data_inicio_quinzena
            and r.lote = o.lote
            and r.consorcio = o.consorcio
    ),
    valorado as (
        select
            *,
            tarifa_remuneracao
            * alpha
            * coalesce(lote_km_referencia, 0.0)
            * fator_cumprimento_frota as remuneracao_capex_quinzena
        from base
    ),
    bruto as (
        select
            *,
            remuneracao_capex_quinzena
            + remuneracao_opex_quinzena
            - receita_tarifa_publica_quinzena
            - coalesce(
                desconto_operacao_precaria_quinzena, 0.0
            ) as valor_a_pagar_bruto_quinzena
        from valorado
    )
select
    data_inicio_quinzena,
    data_fim_quinzena,
    lote,
    consorcio,
    desconto_operacao_precaria_quinzena,
    remuneracao_opex_quinzena,
    fator_cumprimento_frota,
    remuneracao_capex_quinzena,
    receita_tarifa_publica_quinzena,
    valor_a_pagar_bruto_quinzena,
    valor_a_pagar_bruto_quinzena * 0.02 as valor_imposto_iss,
    valor_a_pagar_bruto_quinzena * 0.024 as valor_imposto_irrf,
    valor_a_pagar_bruto_quinzena
    - (valor_a_pagar_bruto_quinzena * 0.02)
    - (valor_a_pagar_bruto_quinzena * 0.024) as valor_subsidio_liquido_quinzena,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from bruto
