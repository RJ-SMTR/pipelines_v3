{{
    config(
        materialized="view",
        tags=["geolocalizacao"],
    )
}}

select
    data,
    hora,
    datetime_gps as timestamp_gps,
    id_veiculo,
    servico,
    latitude,
    longitude,
    status,
    velocidade_instantanea,
    velocidade_estimada_10_min,
    distancia,
    versao,
    datetime_ultima_atualizacao
from {{ ref("view_gps_onibus") }}
where sistema in ("SPPO", "SPPO Executivo")
