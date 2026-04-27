{{ config(materialized="ephemeral") }}

with
    routes as (
        select
            *,
            case
                when agency_id in ("22005", "22002", "22004", "22003")
                then "Ônibus"
                when agency_id = "20001"
                then "BRT"
            end as modo
        from {{ ref("routes_gtfs") }}
    )
select
    t.trip_id,
    r.modo,
    t.route_id,
    t.service_id,
    r.route_short_name as servico,
    t.direction_id,
    t.shape_id,
    t.feed_version,
    t.feed_start_date,
    regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
from {{ ref("trips_gtfs") }} t
join routes r using (feed_start_date, feed_version, route_id)
