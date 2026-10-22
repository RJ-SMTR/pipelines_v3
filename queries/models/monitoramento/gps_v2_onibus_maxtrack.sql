{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        require_partition_filter=true,
    )
}}

{{
    gps_tratamento_v2(
        ref("staging_gps_v2_maxtrack"), ref("aux_gps_filtrada_v2_onibus_maxtrack")
    )
}}
