{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  FCF tipológico por lote × quinzena (Anexo I.8 item 4).
  Frota operante observada a tipología: distinct veículos completa+pico
  por tecnologia_fcf; denominador = operacao_lote_tecnologia.

  tecnologia_fcf = coalesce(apurada, mínima) — eco de
  viagem_classificacao_validacao / viagens_apuradas.
  fcf = min(1, Σ min(media_operante_tech, estimada_tech) / Σ estimada_tech)

  Quinzena: 1 = dias 1–15; 2 = dias 16–fim.
  I.2 (frota/QR/km_referencia) via operacao_lote na data de referência
  da quinzena — mesmo padrão de operacao_lote_tecnologia.
#}
with
    viagens as (
        select
            data,
            lote,
            lower(tecnologia_fcf) as tecnologia_fcf,
            id_veiculo,
            indicador_completa_pico_manha,
            indicador_completa_pico_tarde,
            indicador_dia_util
        from {{ ref("viagens_apuradas") }}
    ),
    frota_dia_tech as (
        select
            data,
            lote,
            tecnologia_fcf,
            max(indicador_dia_util) as indicador_dia_util,
            count(
                distinct if(indicador_completa_pico_manha, id_veiculo, null)
            ) as frota_pico_manha,
            count(
                distinct if(indicador_completa_pico_tarde, id_veiculo, null)
            ) as frota_pico_tarde
        from viagens
        where tecnologia_fcf is not null
        group by data, lote, tecnologia_fcf
    ),
    frota_dia_tech_fc as (
        select
            data,
            lote,
            tecnologia_fcf,
            indicador_dia_util,
            case
                when indicador_dia_util
                then greatest(frota_pico_manha, frota_pico_tarde)
                else 0.0
            end as frota_operante,
            extract(year from data) as ano,
            extract(month from data) as mes,
            if(extract(day from data) <= 15, 1, 2) as quinzena
        from frota_dia_tech
    ),
    media_tech as (
        select
            ano,
            mes,
            quinzena,
            lote,
            tecnologia_fcf,
            avg(if(indicador_dia_util, frota_operante, null)) as frota_operante_media,
            countif(indicador_dia_util) as qtd_dias_uteis,
            min(data) as data_ref_quinzena
        from frota_dia_tech_fc
        group by ano, mes, quinzena, lote, tecnologia_fcf
    ),
    -- dias úteis da quinzena (mesmo se só um tech tiver frota)
    dias_uteis_lote as (
        select ano, mes, quinzena, lote, max(qtd_dias_uteis) as qtd_dias_uteis
        from media_tech
        group by ano, mes, quinzena, lote
    ),
    lote_quinzena as (
        select ano, mes, quinzena, lote, min(data_ref_quinzena) as data_ref_quinzena
        from media_tech
        group by ano, mes, quinzena, lote
    ),
    operacao as (
        select
            lq.ano,
            lq.mes,
            lq.quinzena,
            lq.lote,
            op.frota_estimada as lote_frota_estimada,
            op.frota_determinada as lote_frota_determinada,
            op.qr_mensal as lote_qr_mensal,
            op.km_referencia as lote_km_referencia
        from lote_quinzena lq
        left join
            {{ ref("operacao_lote") }} as op
            on op.lote = lq.lote
            and (op.data_inicio is null or lq.data_ref_quinzena >= op.data_inicio)
            and (op.data_fim is null or lq.data_ref_quinzena <= op.data_fim)
    ),
    estimada_tech as (
        select
            lq.ano,
            lq.mes,
            lq.quinzena,
            lq.lote,
            lower(olt.tipo_veiculo) as tecnologia_fcf,
            cast(olt.frota_estimada as float64) as frota_estimada_tech,
            cast(olt.frota_determinada as float64) as frota_determinada_tech
        from lote_quinzena lq
        inner join
            {{ ref("operacao_lote_tecnologia") }} as olt
            on olt.lote = lq.lote
            and (olt.data_inicio is null or lq.data_ref_quinzena >= olt.data_inicio)
            and (olt.data_fim is null or lq.data_ref_quinzena <= olt.data_fim)
    ),
    tipologico as (
        select
            et.ano,
            et.mes,
            et.quinzena,
            et.lote,
            et.tecnologia_fcf,
            coalesce(mt.frota_operante_media, 0.0) as frota_operante_media,
            case
                when et.frota_determinada_tech is not null
                then least(et.frota_estimada_tech, et.frota_determinada_tech)
                else et.frota_estimada_tech
            end as frota_estimada_tech
        from estimada_tech et
        left join
            media_tech mt
            on mt.ano = et.ano
            and mt.mes = et.mes
            and mt.quinzena = et.quinzena
            and mt.lote = et.lote
            and mt.tecnologia_fcf = et.tecnologia_fcf
    ),
    agregado as (
        select
            t.ano,
            t.mes,
            t.quinzena,
            t.lote,
            sum(t.frota_operante_media) as frota_operante_media,
            sum(t.frota_estimada_tech) as frota_estimada,
            sum(
                least(t.frota_operante_media, t.frota_estimada_tech)
            ) as frota_numerador,
            max(d.qtd_dias_uteis) as qtd_dias_uteis,
            max(op.lote_frota_estimada) as lote_frota_estimada,
            max(op.lote_frota_determinada) as lote_frota_determinada,
            max(op.lote_qr_mensal) as lote_qr_mensal,
            max(op.lote_km_referencia) as lote_km_referencia
        from tipologico t
        left join
            dias_uteis_lote d
            on d.ano = t.ano
            and d.mes = t.mes
            and d.quinzena = t.quinzena
            and d.lote = t.lote
        left join
            operacao op
            on op.ano = t.ano
            and op.mes = t.mes
            and op.quinzena = t.quinzena
            and op.lote = t.lote
        group by t.ano, t.mes, t.quinzena, t.lote
    )
select
    ano,
    mes,
    quinzena,
    lote,
    frota_operante_media,
    qtd_dias_uteis,
    frota_estimada,
    least(
        1.0, coalesce(safe_divide(frota_numerador, frota_estimada), 0.0)
    ) as fcf_quinzena,
    coalesce(lote_km_referencia, safe_divide(lote_qr_mensal, 2.0)) as km_referencia,
    coalesce(safe_divide(lote_qr_mensal, 2.0), lote_km_referencia) as qr_quinzena,
    lote_frota_estimada,
    lote_frota_determinada,
    lote_qr_mensal,
    lote_km_referencia
from agregado
