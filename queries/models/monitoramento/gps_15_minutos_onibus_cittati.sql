{{
    config(
        enabled=false,
        materialized="table",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
    )
}}

{{
    gps_15_minutos_tratamento(
        ref("staging_gps_cittati"), ref("staging_realocacao_cittati")
    )
}}
