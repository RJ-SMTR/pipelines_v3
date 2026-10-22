{{ config(materialized="ephemeral") }}

select
    safe_cast(data_inicio as date) as data_inicio,
    safe_cast(data_fim as date) as data_fim,
    safe_cast(perc_km_inferior as integer) as perc_km_inferior,
    safe_cast(perc_km_superior as integer) as perc_km_superior,
    nullif(safe_cast(tipo_penalidade as string), '') as tipo_penalidade,
    safe_cast(nullif(trim(valor), '') as numeric) as valor,
    nullif(safe_cast(legislacao as string), '') as legislacao,
from {{ source("subsidio_staging", "valor_tipo_penalidade") }}
