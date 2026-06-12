{{
    config(
        materialized=("ephemeral" if var("15_minutos") else "incremental"),
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        cluster_by=["datetime_gps"],
        alias=this.name ~ "_" ~ var("modo_gps") ~ "_" ~ var("fonte_gps"),
        tags=["geolocalizacao"],
        require_partition_filter=true,
    )
}}

{% set staging_gps = ref("staging_gps") %}
{% if execute and is_incremental() %}
    {% set gps_partitions_query %}
    select distinct concat("'", date(data), "'") as data
    from {{ staging_gps }}
    where
        {{
            generate_date_hour_partition_filter(
                var("date_range_start"), var("date_range_end")
            )
        }}
    {% endset %}

    {% set gps_partitions = run_query(gps_partitions_query).columns[0].values() %}
{% endif %}

with
    box as (
        /* Geometria de caixa que contém a área do município de Rio de Janeiro.*/
        select min_longitude, min_latitude, max_longitude, max_latitude
        from {{ ref("limites_geograficos_caixa") }}
    ),
    gps as (
        select *, st_geogpoint(longitude, latitude) posicao_veiculo_geo
        from {{ ref("aux_gps_realocacao") }}
    ),
    filtrada as (
        select
            data,
            hora,
            datetime_gps,
            id_veiculo,
            servico,
            latitude,
            longitude,
            posicao_veiculo_geo,
            datetime_captura,
            velocidade,
        from gps
        cross join box
        where
            st_intersectsbox(
                posicao_veiculo_geo,
                min_longitude,
                min_latitude,
                max_longitude,
                max_latitude
            )
    ),
    particoes_completas as (
        select *, 0 as priority
        from filtrada

        {% if is_incremental() and gps_partitions | length > 0 %}
            union all

            select *, 1 as priority
            from {{ this }}
            where data in ({{ gps_partitions | join(", ") }})
        {% endif %}
    )
select * except (priority)
from particoes_completas
qualify row_number() over (partition by id_veiculo, datetime_gps order by priority) = 1
