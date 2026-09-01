{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  RQ contratual por lote × quinzena (Anexo I.8 item 3).
  OPEX: Σ remuneracao_opex_viagem (OF).
  CAPEX: TR × α × km_referencia × fcf_quinzena.
  Precária: Σ distinct por faixa (valores broadcast na viagem)
  + R$ 1.200 por faixa POR sem viagem nos dias apurados
  (I.8 item 6.2, % = 0, operação precária grave).
  prd = 0: stub IDT = 1 (Anexo I.7 ainda não existe) → PRD = 0% → (1-PRD)=1.
#}
with
    viagens as (
        select
            *,
            extract(year from data) as ano,
            extract(month from data) as mes,
            if(extract(day from data) <= 15, 1, 2) as quinzena
        from {{ ref("viagens_apuradas") }}
    ),
    faixas as (
        select distinct
            ano,
            mes,
            quinzena,
            lote,
            data,
            servico,
            sentido,
            faixa_horaria_inicio,
            qc_km_ponderada_ipa,
            desconto_operacao_precaria
        from viagens
    ),
    agg_faixa as (
        select
            ano,
            mes,
            quinzena,
            lote,
            sum(qc_km_ponderada_ipa) as km_ponderada_ipa,
            sum(desconto_operacao_precaria) as desconto_operacao_precaria_total
        from faixas
        group by ano, mes, quinzena, lote
    ),
    agg_opex as (
        select
            ano,
            mes,
            quinzena,
            lote,
            sum(remuneracao_opex_viagem) as parcela_opex,
            sum(km_remuneravel) as km_remuneravel_total,
            max(tarifa_remuneracao) as tarifa_remuneracao,
            max(alpha) as alpha,
            max(beta) as beta,
            count(*) as qtd_viagens
        from viagens
        group by ano, mes, quinzena, lote
    ),
    dias_apurados as (select distinct data from viagens),
    faixas_com_viagem as (
        select distinct data, servico, sentido, faixa_horaria_inicio from viagens
    ),
    -- I.8 item 6.2: faixa POR com programadas e sem viagem no dia apurado → R$ 1200
    faixas_vazias as (
        select
            extract(year from o.data) as ano,
            extract(month from o.data) as mes,
            if(extract(day from o.data) <= 15, 1, 2) as quinzena,
            o.lote
        from {{ ref("servico_oferta_faixa") }} as o
        inner join dias_apurados d on d.data = o.data
        left join
            faixas_com_viagem v
            on v.data = o.data
            and v.servico = o.servico
            and v.sentido = o.sentido
            and v.faixa_horaria_inicio = format(
                '%02d:%02d',
                extract(hour from o.faixa_horaria_inicio),
                extract(minute from o.faixa_horaria_inicio)
            )
        where o.viagens_programadas > 0 and v.data is null
    ),
    agg_faixa_vazia as (
        select ano, mes, quinzena, lote, count(*) * 1200.0 as desconto_faixa_vazia
        from faixas_vazias
        group by ano, mes, quinzena, lote
    ),
    base as (
        select
            o.ano,
            o.mes,
            o.quinzena,
            o.lote,
            o.tarifa_remuneracao,
            o.alpha,
            o.beta,
            o.parcela_opex,
            o.km_remuneravel_total,
            o.qtd_viagens,
            f.km_ponderada_ipa,
            coalesce(f.desconto_operacao_precaria_total, 0.0) + coalesce(
                vz.desconto_faixa_vazia, 0.0
            ) as desconto_operacao_precaria_total,
            c.frota_operante_media,
            c.frota_estimada,
            c.fcf_quinzena,
            c.km_referencia,
            c.qr_quinzena,
            c.qtd_dias_uteis
        from agg_opex as o
        inner join agg_faixa as f using (ano, mes, quinzena, lote)
        inner join {{ ref("fcf_quinzena_lote") }} as c using (ano, mes, quinzena, lote)
        left join agg_faixa_vazia as vz using (ano, mes, quinzena, lote)
    )
select
    ano,
    mes,
    quinzena,
    lote,
    tarifa_remuneracao,
    alpha,
    beta,
    -- stub IDT = 1 (I.7 ainda não existe) → PRD = 0% → (1 − PRD) = 1
    cast(0.0 as float64) as prd,
    fcf_quinzena,
    km_referencia,
    qr_quinzena,
    frota_operante_media,
    frota_estimada,
    qtd_dias_uteis,
    km_ponderada_ipa,
    km_remuneravel_total,
    qtd_viagens,
    desconto_operacao_precaria_total,
    tarifa_remuneracao
    * alpha
    * coalesce(km_referencia, 0.0)
    * fcf_quinzena as parcela_capex,
    parcela_opex,
    (tarifa_remuneracao * alpha * coalesce(km_referencia, 0.0) * fcf_quinzena)
    + parcela_opex
    - coalesce(desconto_operacao_precaria_total, 0.0) as remuneracao_servico
from base
