-- Purpose: one row per vehicle visit to a GTFS stop, derived from GPS pings. This is
-- the stop-based counterpart of h3_gps.
-- The GPS feeds report only the service (route short name), not the GTFS route_id
-- or direction. Each ping is therefore matched to the stops of every route and
-- direction carrying that service, keeping the nearest stop per route direction
-- within max_snap_m metres. Consecutive pings on the same stop collapse into one
-- visit with an arrival (first ping) and departure (last ping) time.
-- Both directions are kept for every vehicle; the consumer picks the direction,
-- e.g. from the transaction's sentido. Because the two directions' stops sit
-- across the road from each other, every ping also produces a mirror visit in the
-- opposite direction whose stop sequence runs backwards. is_forward marks visits
-- whose sequence rises relative to the vehicle's neighbouring visits in the same
-- route direction, i.e. the direction the bus was really travelling.
-- SPPO only: the BRT GPS view carries no service usable for matching.
--
-- Performance: the spatial join runs first, against one row per stop of the
-- active feed, so BigQuery can use its spatial join optimisation. Route, direction
-- and stop order are attached afterwards by an equality join. Joining route_stops
-- directly with st_dwithin in the same predicate defeats the optimisation and the
-- feed filter is not pushed down, which made the first version run for hours.
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day",
        },
        require_partition_filter=true,
    )
}}

{% set max_snap_m = 300 %}
{% set feed_info = ref("feed_info_gtfs") %}

{% if execute and is_incremental() %}

    {% set gtfs_feeds_query %}
        select concat("'", feed_start_date, "'") as feed_start_date
        from {{ feed_info }}
        where
            feed_start_date <= date("{{ var('date_range_end') }}")
            and (feed_end_date IS NULL OR feed_end_date >= date("{{ var('date_range_start') }}"))
    {% endset %}

    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}

{% endif %}

with
    -- Distinct pings: the view unions several providers and repeats records
    gps as (
        select distinct
            data as service_date,
            datetime_gps,
            id_veiculo as vehicle_id,
            servico as service,
            latitude,
            longitude
        from {{ ref("view_gps_onibus") }}
        where
            {% if is_incremental() %}
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} data >= '{{ var("mit_ipea_project_initial_date") }}'

            {% endif %} and servico is not null
    ),
    active_route_stops as (
        select *
        from {{ ref("aux_route_stops") }}
        {% if is_incremental() %}
            where
                {% if gtfs_feeds | length > 0 %}
                    feed_start_date in ({{ gtfs_feeds | join(", ") }})
                {% else %} false
                {% endif %}
            {% else %} feed_start_date >= '{{ var("mit_ipea_project_initial_date") }}'

            {% endif %}
    ),
    -- One row per stop of the active feed
    active_stops as (
        select
            feed_start_date, feed_end_date, stop_id, any_value(geography) as geography
        from active_route_stops
        group by all
    ),
    -- Spatial join only: every stop within max_snap_m of each ping
    ping_near_stop as (
        select
            gps.service_date,
            gps.datetime_gps,
            extract(time from gps.datetime_gps) as ping_time,
            gps.vehicle_id,
            gps.service,
            s.stop_id,
            s.geography as stop_geography,
            st_distance(
                st_geogpoint(gps.longitude, gps.latitude), s.geography
            ) as distance_m
        from gps
        join
            active_stops as s
            on st_dwithin(
                st_geogpoint(gps.longitude, gps.latitude), s.geography, {{ max_snap_m }}
            )
            and gps.service_date >= s.feed_start_date
            and (gps.service_date <= s.feed_end_date or s.feed_end_date is null)
    ),
    -- Attach route direction and stop order, then keep the nearest stop per route
    -- direction serving the ping's service
    nearest_stop as (
        select p.*, rs.feed_version, rs.route_id, rs.direction_id, rs.stop_sequence
        from ping_near_stop as p
        join
            active_route_stops as rs
            on rs.stop_id = p.stop_id
            and rs.servico = p.service
        qualify
            row_number() over (
                partition by vehicle_id, datetime_gps, rs.route_id, rs.direction_id
                order by distance_m
            )
            = 1
    ),
    -- A visit starts whenever the stop changes between consecutive pings of the
    -- same vehicle, route and direction
    visit_flags as (
        select
            *,
            if(
                lag(stop_id) over (
                    partition by vehicle_id, route_id, direction_id
                    order by datetime_gps
                )
                = stop_id,
                0,
                1
            ) as new_visit
        from nearest_stop
    ),
    visits as (
        select
            *,
            sum(new_visit) over (
                partition by vehicle_id, route_id, direction_id order by datetime_gps
            ) as visit_id
        from visit_flags
    ),
    visit_bounds as (
        select
            service_date,
            feed_version,
            vehicle_id,
            service,
            route_id,
            direction_id,
            stop_id,
            stop_sequence,
            min(ping_time) as arrival_time,
            max(ping_time) as departure_time,
            min(distance_m) as min_distance_m,
            count(*) as n_pings,
            any_value(stop_geography) as geography
        from visits
        group by
            service_date,
            feed_version,
            vehicle_id,
            service,
            route_id,
            direction_id,
            stop_id,
            stop_sequence,
            visit_id
    )
select
    *,
    coalesce(
        stop_sequence > lag(stop_sequence) over (
            partition by vehicle_id, route_id, direction_id order by arrival_time
        ),
        lead(stop_sequence) over (
            partition by vehicle_id, route_id, direction_id order by arrival_time
        )
        > stop_sequence
    ) as is_forward
from visit_bounds
