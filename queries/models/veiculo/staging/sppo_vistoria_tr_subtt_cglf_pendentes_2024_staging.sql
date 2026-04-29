{{ config(alias="sppo_vistoria_tr_subtt_cglf_pendentes_2024") }}

select
    safe_cast(data as date) as data,
    safe_cast(id_veiculo as string) as id_veiculo,
    safe_cast(placa as string) as placa,
    safe_cast(empresa as string) as empresa,
    safe_cast(ano_ultima_vistoria as int64) as ano_ultima_vistoria,
from {{ source("veiculo_staging", "sppo_vistoria_tr_subtt_cglf_pendentes_2024") }}
