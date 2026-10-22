{{
    config(
        alias="tecnologia_servico",
    )
}}

select distinct
    nullif(trim(safe_cast(inicio_vigencia as string)), "") as inicio_vigencia,
    nullif(trim(safe_cast(fim_vigencia as string)), "") as fim_vigencia,
    trim(safe_cast(linha as string)) as servico,
    safe_cast(null as string) as modo,
    nullif(trim(safe_cast(codigo_tecnologia as string)), "") as codigo_tecnologia
from {{ source("planejamento_staging", "tecnologia_servico") }}
