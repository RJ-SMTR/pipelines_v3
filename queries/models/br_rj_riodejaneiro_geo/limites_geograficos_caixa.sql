{{
    config(
        materialized="view",
        alias="limite_geografico_caixa",
    )
}}

select max_latitude, min_latitude, max_longitude, min_longitude
from {{ source("br_rj_riodejaneiro_geo", "limites_geograficos_caixa") }}
