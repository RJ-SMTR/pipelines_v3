{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  RQ contratual por lote × quinzena (Anexo I.8 item 3).
  OPEX: Σ remuneracao_opex_viagem (OF, prd=0).
  CAPEX: TR × α × km_referencia × fcf_quinzena.
  Precária: Σ distinct por faixa (valores broadcast na viagem).
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
            f.desconto_operacao_precaria_total,
            c.frota_operante_media,
            c.frota_estimada,
            c.fcf_quinzena,
            c.km_referencia,
            c.qr_quinzena,
            c.qtd_dias_uteis
        from agg_opex as o
        inner join agg_faixa as f using (ano, mes, quinzena, lote)
        inner join {{ ref("fcf_quinzena_lote") }} as c using (ano, mes, quinzena, lote)
    )
select
    ano,
    mes,
    quinzena,
    lote,
    tarifa_remuneracao,
    alpha,
    beta,
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
