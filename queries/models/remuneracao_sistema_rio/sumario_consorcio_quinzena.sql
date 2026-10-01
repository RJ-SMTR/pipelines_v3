{{
    config(
        materialized="table",
        partition_by={
            "field": "data_inicio_quinzena",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

{% set dias_capex %}
    case
        when
            data_inicio_quinzena = date '2026-08-16'
            and data_fim_quinzena = date '2026-08-31'
        then date_diff(date '2026-08-31', date '2026-08-24', day) + 1
        else date_diff(data_fim_quinzena, data_inicio_quinzena, day) + 1
    end
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
            lote_km_referencia_quinzena,
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
            max(lote_km_referencia_quinzena) as lote_km_referencia_quinzena,
            max(tarifa_remuneracao) as tarifa_remuneracao,
            max(alpha) as alpha
        from dia_lote
        group by data_inicio_quinzena, lote
    ),
    receita_dia as (
        select
            date_sub(data_ordem, interval 1 day) as data,
            consorcio,
            sum(valor_total_transacao_bruto) as receita_tarifa_publica_dia
        -- from {{ ref("bilhetagem_consorcio_operador_dia") }}
        from `rj-smtr.financeiro.bilhetagem_consorcio_operador_dia`
        where
            data_ordem between date_add(
                date('{{ var("date_range_start") }}'), interval 1 day
            ) and date_add(date('{{ var("date_range_end") }}'), interval 1 day)
        group by data, consorcio
    ),
    receita_quinzena as (
        select
            if(
                extract(day from data) <= 15,
                date_trunc(data, month),
                date_add(date_trunc(data, month), interval 15 day)
            ) as data_inicio_quinzena,
            {{ lote_consorcio_rio("consorcio") }} as lote,
            consorcio,
            sum(receita_tarifa_publica_dia) as receita_tarifa_publica_quinzena
        from receita_dia
        group by data_inicio_quinzena, lote, consorcio
    ),
    base as (
        select
            o.data_inicio_quinzena,
            o.data_fim_quinzena,
            o.lote,
            o.consorcio,
            o.desconto_operacao_precaria_quinzena,
            o.remuneracao_opex_quinzena,
            if(
                o.data_fim_quinzena <= date('2026-09-30'),
                1.0,
                least(
                    1.0,
                    coalesce(
                        safe_divide(f.frota_operante_media, f.lote_frota_estimada), 0.0
                    )
                )
            ) as fator_cumprimento_frota,
            f.tarifa_remuneracao,
            f.alpha,
            f.lote_km_referencia_quinzena,
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
            * coalesce(lote_km_referencia_quinzena, 0.0)
            / 15
            * ({{ dias_capex }})
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
    ),
    imposto as (
        select *, greatest(valor_a_pagar_bruto_quinzena, 0.0) as base_imposto from bruto
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
    base_imposto * 0.02 as valor_imposto_iss,
    base_imposto * 0.024 as valor_imposto_irrf,
    valor_a_pagar_bruto_quinzena
    - (base_imposto * 0.02)
    - (base_imposto * 0.024) as valor_subsidio_liquido_quinzena,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from imposto
