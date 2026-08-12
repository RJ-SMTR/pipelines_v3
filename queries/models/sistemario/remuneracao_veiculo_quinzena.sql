{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Rateio da RQ por veículo × lote × quinzena.
  OPEX: Σ remuneracao_opex_viagem (OF).
  CAPEX v1: proporcional a km_remuneravel no lote×quinzena.
  Desconto precária: proporcional a km_ponderada_ipa_viagem.
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
    por_veiculo as (
        select
            ano,
            mes,
            quinzena,
            lote,
            id_veiculo,
            count(*) as qtd_viagens,
            sum(km_remuneravel) as km_remuneravel,
            sum(km_ponderada_ipa_viagem) as km_ponderada_ipa_viagem,
            sum(remuneracao_opex_viagem) as remuneracao_opex
        from viagens
        group by ano, mes, quinzena, lote, id_veiculo
    ),
    com_lote as (
        select
            v.*,
            r.parcela_capex,
            r.parcela_opex,
            r.desconto_operacao_precaria_total,
            r.remuneracao_servico,
            r.fcf_quinzena,
            r.tarifa_remuneracao,
            r.alpha,
            r.beta,
            r.prd,
            sum(v.km_remuneravel) over (
                partition by v.ano, v.mes, v.quinzena, v.lote
            ) as km_remuneravel_lote,
            sum(v.km_ponderada_ipa_viagem) over (
                partition by v.ano, v.mes, v.quinzena, v.lote
            ) as km_ponderada_ipa_lote
        from por_veiculo as v
        inner join
            {{ ref("remuneracao_quinzena_lote") }} as r using (ano, mes, quinzena, lote)
    )
select
    ano,
    mes,
    quinzena,
    lote,
    id_veiculo,
    qtd_viagens,
    km_remuneravel,
    km_ponderada_ipa_viagem,
    remuneracao_opex,
    coalesce(safe_divide(km_remuneravel, km_remuneravel_lote), 0.0) as peso_capex,
    parcela_capex * coalesce(
        safe_divide(km_remuneravel, km_remuneravel_lote), 0.0
    ) as remuneracao_capex,
    coalesce(
        safe_divide(km_ponderada_ipa_viagem, km_ponderada_ipa_lote), 0.0
    ) as peso_desconto,
    desconto_operacao_precaria_total * coalesce(
        safe_divide(km_ponderada_ipa_viagem, km_ponderada_ipa_lote), 0.0
    ) as desconto_operacao_precaria,
    (parcela_capex * coalesce(safe_divide(km_remuneravel, km_remuneravel_lote), 0.0))
    + remuneracao_opex
    - (
        desconto_operacao_precaria_total
        * coalesce(safe_divide(km_ponderada_ipa_viagem, km_ponderada_ipa_lote), 0.0)
    ) as remuneracao_veiculo,
    fcf_quinzena,
    tarifa_remuneracao,
    alpha,
    beta,
    prd,
    parcela_capex,
    parcela_opex,
    remuneracao_servico
from com_lote
