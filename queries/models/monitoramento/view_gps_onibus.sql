{{
    config(
        materialized="view",
        tags=["geolocalizacao"],
    )
}}

select *, 'conecta' as fonte_gps
from {{ source("monitoramento", "gps_onibus_conecta") }}

union all

select *, 'zirix' as fonte_gps
from
    {{ source("monitoramento", "gps_onibus_zirix") }}

    -- union all
    -- select *, 'cittati' as fonte_gps
    -- from {{ source("monitoramento", "gps_onibus_cittati") }}
