{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        cluster_by=["datetime_gps"],
        tags=["geolocalizacao"],
        require_partition_filter=true,
    )
}}

{{ gps_aux_filtrada(ref("staging_gps_v2_conecta_sistema_rio"), versao=2) }}
