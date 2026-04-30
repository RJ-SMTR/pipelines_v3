{{
    config(
        materialized="view",
        alias="valor_km_tipo_viagem",
    )
}}

select
    safe_cast(status as string) as status,
    nullif(safe_cast(tecnologia as string), '') as tecnologia,
    safe_cast(subsidio_km as numeric) as subsidio_km,
    safe_cast(desconto_subsidio_km as numeric) as desconto_subsidio_km,
    safe_cast(irk as numeric) as irk,
    safe_cast(irk_tarifa_publica as numeric) as irk_tarifa_publica,
    safe_cast(data_inicio as date) as data_inicio,
    safe_cast(data_fim as date) as data_fim,
    safe_cast(indicador_penalidade_judicial as bool) as indicador_penalidade_judicial,
    safe_cast(indicador_conformidade as bool) as indicador_conformidade,
    safe_cast(indicador_validade as bool) as indicador_validade,
    nullif(safe_cast(legislacao as string), '') as legislacao,
    safe_cast(ordem as int64) as ordem
from {{ source("subsidio_staging", "valor_km_tipo_viagem") }}
