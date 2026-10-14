{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        require_partition_filter=true,
    )
}}

{{ gps_tratamento(ref("staging_gps_zirix"), ref("aux_gps_filtrada_onibus_zirix")) }}
