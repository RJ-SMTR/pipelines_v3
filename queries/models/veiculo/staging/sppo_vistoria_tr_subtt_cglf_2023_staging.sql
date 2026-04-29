{{ config(alias="sppo_vistoria_tr_subtt_cglf_2023") }}

select
    safe_cast(data as date) as data,
    safe_cast(id_veiculo as string) as id_veiculo,
    safe_cast(placa as string) as placa,
    safe_cast(permissao as string) as permissao,
    safe_cast(chassi as string) as chassi,
    safe_cast(ano_fabricacao as int64) as ano_fabricacao,
    safe_cast(selo as string) as selo,
    safe_cast(darm as string) as darm,
    safe_cast(ano_ultima_vistoria as int64) as ano_ultima_vistoria,
from {{ source("veiculo_staging", "sppo_vistoria_tr_subtt_cglf_2023") }}
