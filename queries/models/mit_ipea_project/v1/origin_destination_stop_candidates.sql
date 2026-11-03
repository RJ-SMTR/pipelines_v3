-- Survey options for the stop-based alighting prediction: the predicted stop plus
-- n_alternates neighbouring stops on the same route direction, one row per trip and
-- option. Alternates are chosen by sequence distance from the prediction, nearest
-- first (the stop before and the stop after), then by walking distance to the
-- rider's next boarding location to break ties (e.g. which of the two stops two
-- places away), then by stop sequence so loop routes that visit a stop twice rank
-- deterministically. Only stops after the boarding stop are eligible. Trips whose
-- prediction sits at the end of the route get alternates on the other side.
-- display_order is a deterministic shuffle so the prediction is not always first.
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
            feed_version,
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

    -- The prediction and every stop within max_offset places of it, after boarding.
    -- Stops come from the feed the boarding was matched against (feed_version
    -- travels with the prediction), so options and prediction share one feed
    options as (
        select
            p.*,
            rs.stop_id,
            rs.stop_sequence,
            rs.stop_name,
            rs.stop_lat,
            rs.stop_lon,
            rs.stop_sequence - p.alighting_stop_sequence as sequence_offset,
            st_distance(rs.geography, p.next_boarding_location) as walk_distance_m
        from predictions as p
        join
            {{ ref("route_stops") }} as rs
            on rs.feed_version = p.feed_version
            and rs.route_id = p.route_id
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
                order by
                    sequence_offset = 0 desc,
                    abs(sequence_offset),
                    walk_distance_m,
                    stop_sequence
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
    row_number() over (
        partition by service_date, rider_id, trip_sequence
        order by farm_fingerprint(concat(rider_id, '|', trip_sequence, '|', stop_id))
    ) as display_order
from ranked
where option_rank <= 1 + {{ n_alternates }}
