{{ config(materialized="table") }}

-- Purpose: Create a table that assigns each GPS ping to a H3 tile
-- Rename columns into english, join on H3 table via a circle fully encapsulating each
-- tile
-- Each row is now a set of possible H3 tiles for each observation
with
    gps as (
        select
            stop_id,
            stop_name,
            stop_lat,
            stop_lon,
            st_geogpoint(stop_lon, stop_lat) as geography,
            tile_id
        from `rj-smtr`.gtfs.stops
        left join
            {{ ref("vw_h3") }} as h3
            on st_dwithin(st_geogpoint(stop_lon, stop_lat), h3.centroid, 250)

        where feed_version = (select max(feed_version) from `rj-smtr`.gtfs.stops)
    ),

    h3_gps as (
        select *
        from gps
        left join {{ ref("vw_h3") }} as h3 using (tile_id)

        where st_intersects(gps.geography, h3.geometry) is true

    )
select *
from h3_gps
