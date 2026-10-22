{{
    config(
        materialized="table",
    )
}}

select
    status,
    tecnologia,
    subsidio_km,
    desconto_subsidio_km,
    irk,
    irk_tarifa_publica,
    data_inicio,
    data_fim,
    indicador_penalidade_judicial,
    indicador_conformidade,
    indicador_validade,
    legislacao,
    ordem
from {{ ref("staging_valor_km_tipo_viagem") }}
