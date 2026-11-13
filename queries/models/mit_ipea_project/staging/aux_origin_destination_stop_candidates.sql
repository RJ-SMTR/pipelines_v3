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
        require_partition_filter=true,
    )
}}

{% set n_alternates = 3 %}
{% set max_offset = 3 %}

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
        from {{ ref("aux_origin_destination_stop") }}
        where
            {% if is_incremental() %}
                service_date between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} service_date >= '{{ var("mit_ipea_project_initial_date") }}'

            {% endif %} and alighting_stop_id is not null
    ),
    route_stops as (
        select *
        from {{ ref("aux_route_stops") }}
        {% if is_incremental() %}
            where
                {% if gtfs_feeds | length > 0 %}
                    feed_start_date in ({{ gtfs_feeds | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}

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
            route_stops as rs
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
        order by
            farm_fingerprint(concat(to_hex(rider_id), '|', trip_sequence, '|', stop_id))
    ) as display_order
from ranked
where option_rank <= 1 + {{ n_alternates }}
