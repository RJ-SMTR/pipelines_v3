{{
    config(
        materialized="view",
        schema="br_rj_riodejaneiro_veiculos",
        alias="gps_sppo_15_minutos",
        tags=["geolocalizacao"],
    )
}}


select
    data,
    servico,
    id_veiculo,
    latitude,
    longitude,
    st_geogpoint(longitude, latitude) posicao_veiculo,
    datetime_gps as timestamp_gps,
    case
        when status = "Parado garagem" then 0 when status like "Parado %" then 1 else 0
    end as indicador_veiculo_parado
from {{ source("monitoramento", "gps_15_minutos_onibus_sppo") }}
