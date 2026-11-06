-- Purpose: Create a table that assigns each GPS ping to a H3 tile
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    -- Union DISTINCT, both BRT and SPPO gps tables
    -- The tables do contain some duplicates
    -- SPPO: rj-smtr.monitoramento.view_gps_onibus unions the per-provider feeds
    -- (conecta, zirix, cittati, maxtrack) and is the only source after 2026-08-05.
    -- BRT: rj-smtr.monitoramento.view_gps_brt_completo unions the legacy table
    -- (until 2026-05-06) with gps_brt_sonda.
    -- Neither view carries `modo`, and both expose `hora` as an integer hour, so the
    -- legacy `modo` literals and the TIME-typed `hora` are rebuilt here.
    brt_sppo_gps as (

        select
            'SPPO' as modo,
            datetime_gps as timestamp_gps,
            data,
            extract(time from datetime_gps) as hora,
            id_veiculo,
            servico,
            latitude,
            longitude
        from `rj-smtr.monitoramento.view_gps_onibus`
        where data = "{{ var('run_date') }}"

        union distinct

        select
            'BRT' as modo,
            timestamp_gps,
            data,
            extract(time from timestamp_gps) as hora,
            id_veiculo,
            servico,
            latitude,
            longitude
        from `rj-smtr.monitoramento.view_gps_brt_completo`
        where data = "{{ var('run_date') }}"
    ),

    -- Rename columns into english, join on H3 table via a circle fully encapsulating
    -- each tile
    -- Each row is now a set of possible H3 tiles for each observation
    gps as (
        select
            modo as mode,
            timestamp_gps,
            data as service_date,
            hora as time,
            id_veiculo as vehicle_id,
            servico as service,
            latitude,
            longitude,
            st_geogpoint(longitude, latitude) as geography,
            tile_id
        from brt_sppo_gps
        left join
            {{ ref("vw_h3") }} as h3
            on st_dwithin(st_geogpoint(longitude, latitude), h3.centroid, 250)

    ),

    h3_gps as (
        select
            *,
            case
                when
                    lag(tile_id) over (partition by vehicle_id order by timestamp_gps)
                    = tile_id
                then 0
                else 1
            end as tile_entry  -- 1 when the vehicle first enters the tile. If 0, the vehicle is already in the tile.
        from gps
        left join {{ ref("vw_h3") }} as h3 using (tile_id)

        where st_intersects(gps.geography, h3.geometry) is true

    )
select
    service_date,
    time as tile_entry_time,
    lead(time) over (
        partition by vehicle_id order by service_date, time
    ) as tile_exit_time,
    mode,
    service,
    vehicle_id,
    longitude,
    latitude,
    tile_id,
    centroid

from h3_gps
where tile_entry = 1
