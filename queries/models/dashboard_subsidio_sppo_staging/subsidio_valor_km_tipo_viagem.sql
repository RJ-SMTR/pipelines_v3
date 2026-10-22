{{
    config(
        materialized="view",
    )
}}

select
    safe_cast(status as string) status,
    safe_cast(subsidio_km as float64) subsidio_km,
    safe_cast(irk as float64) irk,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
    safe_cast(indicador_penalidade_judicial as bool) indicador_penalidade_judicial,
    safe_cast(legislacao as string) legislacao
from {{ source("dashboard_subsidio_sppo_staging", "subsidio_valor_km_tipo_viagem") }}
