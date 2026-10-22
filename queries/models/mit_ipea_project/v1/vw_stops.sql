-- GTFS stops as geography, one row per stop per feed version. Stations and
-- entrances (location_type other than 0) are excluded; only boarding points remain.
select
    feed_version,
    feed_start_date,
    feed_end_date,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    st_geogpoint(stop_lon, stop_lat) as geography
from `rj-smtr.gtfs.stops`
where coalesce(location_type, '0') in ('0', '')
