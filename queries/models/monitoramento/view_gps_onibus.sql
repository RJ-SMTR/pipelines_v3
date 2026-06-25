{{
    config(
        materialized="view",
        tags=["geolocalizacao"],
    )
}}

select *, 'sppo' as fonte_gps
from {{ source("monitoramento", "gps_onibus_sppo") }}

union all

select *, 'zirix' as fonte_gps
from
    {{ source("monitoramento", "gps_onibus_zirix") }}

    -- union all
    -- select *, 'cittati' as fonte_gps
    -- from {{ source("monitoramento", "gps_onibus_cittati") }}
    -- union all
    -- select *, 'conecta' as fonte_gps
    -- from {{ source("monitoramento", "gps_onibus_conecta") }}
