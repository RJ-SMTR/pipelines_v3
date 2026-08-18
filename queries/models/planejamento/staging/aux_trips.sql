{{ config(materialized="ephemeral") }}

with
    agency as (
        select feed_start_date, agency_id, agency_name from {{ ref("agency_gtfs") }}
    ),
    routes as (
        select
            r.*,
            a.agency_name as consorcio,
            case
                when r.agency_id in ("22005", "22002", "22004", "22003")
                then "SPPO"
                when regexp_contains(r.agency_id, r"^[A-Z][0-9]$")
                then "RIO"
                else null
            end as sistema,
            case
                when r.agency_id in ("22005", "22002", "22004", "22003")
                then "Ônibus"
                when r.agency_id = "20001"
                then "BRT"
            end as modo
        from {{ ref("routes_gtfs") }} r
        left join agency a using (feed_start_date, agency_id)
    )
select
    t.trip_id,
    r.modo,
    r.consorcio,
    r.sistema,
    r.route_long_name as vista,
    t.route_id,
    t.service_id,
    r.route_short_name as servico,
    t.direction_id,
    t.shape_id,
    t.feed_start_date,
    regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
from {{ ref("trips_gtfs") }} t
join routes r using (feed_start_date, route_id)
