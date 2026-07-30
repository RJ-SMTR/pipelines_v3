{{
    config(
        materialized="table",
        alias="dicionario",
    )
}}
select
    safe_cast(chave as string) as chave,
    safe_cast(cobertura_temporal as string) as cobertura_temporal,
    safe_cast(id_tabela as string) as id_tabela,
    safe_cast(coluna as string) as coluna,
    safe_cast(valor as string) as valor
from {{ source("br_rj_riodejaneiro_rdo_staging", "dicionario") }}
