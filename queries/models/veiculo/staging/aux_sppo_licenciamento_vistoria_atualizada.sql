{{ config(materialized="ephemeral") }}

/* Dados auxiliares de vistoria de ônibus levantados pela Coordenadoria Geral de Licenciamento e Fiscalização (TR/SUBTT/CGLF),
para atualização da data de última vistoria informada no sistema [STU]. */
select data, id_veiculo, placa, max(ano_ultima_vistoria) as ano_ultima_vistoria,
from
    (
        select data, id_veiculo, placa, ano_ultima_vistoria,
        from {{ ref("sppo_vistoria_tr_subtt_cglf_2023_staging") }}
        union all
        select data, id_veiculo, placa, ano_ultima_vistoria,
        from {{ ref("sppo_vistoria_tr_subtt_cglf_2024_staging") }}
        union all
        select data, id_veiculo, placa, ano_ultima_vistoria,
        from {{ ref("sppo_vistoria_tr_subtt_cglf_pendentes_2024_staging") }}
        union all
        select data, id_veiculo, placa, ano_ultima_vistoria,
        from {{ ref("sppo_vistoria_tr_subtt_cmo_recurso_SMTR202404004977_staging") }}
    )
group by 1, 2, 3
