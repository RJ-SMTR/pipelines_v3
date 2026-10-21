-- Survey options for the stop-based alighting prediction: the predicted stop plus
-- n_alternates neighbouring stops on the same route direction, one row per trip and
-- option. Alternates are chosen by sequence distance from the prediction, nearest
-- first (the stop before and the stop after), then by walking distance to the
-- rider's next boarding location to break ties (e.g. which of the two stops two
-- places away). Only stops after the boarding stop are eligible. Trips whose
-- prediction sits at the end of the route get alternates on the other side.
-- display_order is a deterministic shuffle so the prediction is not always first.
-- street_view_url opens Google Street View at the stop, facing the direction of
-- travel (bearing from the previous stop on the route).
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

{% set n_alternates = 3 %}
{% set max_offset = 3 %}

with
    predictions as (
        select
            rider_id,
            service_date,
            trip_sequence,
            chain_position,
            boarding_vehicle_id,
            route_id,
            servico,
            direction_id,
            boarding_stop_id,
            boarding_stop_sequence,
            alighting_stop_id,
            alighting_stop_sequence,
            alighting_time,
            alighting_to_next_boarding_distance_m,
            alighting_within_walk_cap,
            next_boarding_location_source,
            next_boarding_location
        from {{ ref("origin_destination_stop") }}
        where service_date = "{{ var('run_date') }}" and alighting_stop_id is not null
    ),

    active_feed as (
        select feed_version
        from (select distinct feed_version, feed_start_date from `rj-smtr.gtfs.stops`)
        where feed_start_date <= date("{{ var('run_date') }}")
        qualify row_number() over (order by feed_start_date desc) = 1
    ),

    route_stops as (
        select *
        from {{ ref("route_stops") }}
        where feed_version in (select feed_version from active_feed)
    ),

    -- Direction of travel at each stop, for the Street View heading
    route_stops_heading as (
        select
            rs.*,
            coalesce(
                st_azimuth(prev.geography, rs.geography),
                st_azimuth(rs.geography, nxt.geography)
            )
            * 180
            / acos(-1) as heading_deg
        from route_stops as rs
        left join
            route_stops as prev
            on prev.route_id = rs.route_id
            and prev.direction_id = rs.direction_id
            and prev.stop_sequence = rs.stop_sequence - 1
        left join
            route_stops as nxt
            on nxt.route_id = rs.route_id
            and nxt.direction_id = rs.direction_id
            and nxt.stop_sequence = rs.stop_sequence + 1
    ),

    -- The prediction and every stop within max_offset places of it, after boarding
    options as (
        select
            p.*,
            rs.stop_id,
            rs.stop_sequence,
            rs.stop_name,
            rs.stop_lat,
            rs.stop_lon,
            rs.heading_deg,
            rs.stop_sequence - p.alighting_stop_sequence as sequence_offset,
            st_distance(rs.geography, p.next_boarding_location) as walk_distance_m
        from predictions as p
        join
            route_stops_heading as rs
            on rs.route_id = p.route_id
            and rs.direction_id = p.direction_id
            and rs.stop_sequence
            between p.alighting_stop_sequence
            - {{ max_offset }} and p.alighting_stop_sequence
            + {{ max_offset }}
            and rs.stop_sequence > p.boarding_stop_sequence
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by service_date, rider_id, trip_sequence
                order by sequence_offset = 0 desc, abs(sequence_offset), walk_distance_m
            ) as option_rank
        from options
    )

select
    rider_id,
    service_date,
    trip_sequence,
    chain_position,
    boarding_vehicle_id,
    route_id,
    servico,
    direction_id,
    boarding_stop_id,
    alighting_stop_id as predicted_stop_id,
    alighting_time as predicted_alighting_time,
    alighting_to_next_boarding_distance_m as predicted_walk_distance_m,
    alighting_within_walk_cap,
    next_boarding_location_source,
    option_rank,
    case
        when sequence_offset = 0
        then 'prediction'
        when sequence_offset < 0
        then 'before'
        else 'after'
    end as option_role,
    sequence_offset,
    stop_id,
    stop_sequence,
    stop_name,
    stop_lat,
    stop_lon,
    walk_distance_m,
    mod(cast(round(heading_deg) as int64) + 360, 360) as heading_deg,
    row_number() over (
        partition by service_date, rider_id, trip_sequence
        order by farm_fingerprint(concat(rider_id, '|', trip_sequence, '|', stop_id))
    ) as display_order,
    concat(
        'https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=',
        stop_lat,
        ',',
        stop_lon,
        '&heading=',
        mod(cast(round(heading_deg) as int64) + 360, 360)
    ) as street_view_url
from ranked
where option_rank <= 1 + {{ n_alternates }}
