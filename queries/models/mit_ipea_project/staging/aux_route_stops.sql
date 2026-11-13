-- depends_on: {{ ref('feed_info_gtfs') }}
-- Ordered stops per GTFS route direction, per feed version. One representative trip
-- per route and direction (the trip with the most stops) defines the stop order.
-- servico is the route short name, which is what the GPS feeds report.
-- Feeds starting before 2025 are skipped to keep the build cheap; widen the filter
-- if older service dates are needed.
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

with
    trips as (
        select *
        from {{ ref("trips_gtfs") }}
        where
            {% if is_incremental() %}
                feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
            {% else %} feed_start_date >= '{{ var("mit_ipea_project_initial_date") }}'
            {% endif %}
    ),
    stop_times as (
        select *
        from {{ ref("stop_times_gtfs") }}
        where
            {% if is_incremental() %}
                feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
            {% else %} feed_start_date >= '{{ var("mit_ipea_project_initial_date") }}'
            {% endif %}
    ),
    stops as (
        select *
        from {{ ref("aux_vw_stops") }}
        where
            {% if is_incremental() %}
                feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
            {% else %} feed_start_date >= '{{ var("mit_ipea_project_initial_date") }}'
            {% endif %}
    ),
    routes as (
        select *
        from {{ ref("routes_gtfs") }}
        where
            {% if is_incremental() %}
                feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
            {% else %} feed_start_date >= '{{ var("mit_ipea_project_initial_date") }}'
            {% endif %}
    ),
    trip_stops as (
        select
            t.feed_version,
            t.feed_start_date,
            t.feed_end_date,
            t.route_id,
            safe_cast(t.direction_id as int64) as direction_id,
            t.trip_id,
            st.stop_id,
            st.stop_sequence,
            count(*) over (partition by t.feed_version, t.trip_id) as n_stops
        from trips as t
        join
            stop_times as st
            on st.feed_version = t.feed_version
            and st.trip_id = t.trip_id
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
    r.feed_start_date,
    r.feed_end_date,
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
join stops as s on s.feed_version = r.feed_version and s.stop_id = r.stop_id
join routes as rt on rt.feed_version = r.feed_version and rt.route_id = r.route_id
