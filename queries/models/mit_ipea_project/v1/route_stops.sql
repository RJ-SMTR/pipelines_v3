-- Ordered stops per GTFS route direction, per feed version. One representative trip
-- per route and direction (the trip with the most stops) defines the stop order.
-- servico is the route short name, which is what the GPS feeds report.
-- Feeds starting before 2025 are skipped to keep the build cheap; widen the filter
-- if older service dates are needed.
{{ config(materialized="table") }}

with
    trip_stops as (
        select
            t.feed_version,
            t.route_id,
            safe_cast(t.direction_id as int64) as direction_id,
            t.trip_id,
            st.stop_id,
            st.stop_sequence,
            count(*) over (partition by t.feed_version, t.trip_id) as n_stops
        from `rj-smtr.gtfs.trips` as t
        join
            `rj-smtr.gtfs.stop_times` as st
            on st.feed_version = t.feed_version
            and st.trip_id = t.trip_id
        where t.feed_start_date >= date('2025-01-01')
    ),

    representative as (
        select *
        from trip_stops
        qualify
            dense_rank() over (
                partition by feed_version, route_id, direction_id
                order by n_stops desc, trip_id
            )
            = 1
    )

select
    r.feed_version,
    r.route_id,
    rt.route_short_name as servico,
    r.direction_id,
    r.trip_id as representative_trip_id,
    r.stop_id,
    r.stop_sequence,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.geography
from representative as r
join
    {{ ref("vw_stops") }} as s
    on s.feed_version = r.feed_version
    and s.stop_id = r.stop_id
join
    `rj-smtr.gtfs.routes` as rt
    on rt.feed_version = r.feed_version
    and rt.route_id = r.route_id
