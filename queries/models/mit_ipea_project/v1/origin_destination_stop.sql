-- Stop-based origin-destination inference. Stop counterpart of origin_destination.
--
-- Route comes from the transaction via the service registry. Direction is taken
-- from the Jaé sentido (0 = ida, 1 = volta, same as GTFS) when the boarding vehicle
-- has a forward stop visit in that direction near the tap time; otherwise the
-- opposite direction is tried, since sentido is wrong for a share of taps and the
-- GPS trend is the ground truth. direction_source records which applied.
--
-- 1. Boarding stop: the boarding vehicle's forward visit (see stop_events.is_forward)
-- to a stop of the transaction's route closest in time to the tap, within
-- max_boarding_gap_s seconds (tolerance for rounded tap times), preferring the
-- stated direction. Stops across the road are never used.
-- 2. Trip chaining: the next tap's boarding stop is the rider's next origin. For the
-- last tap of the day the first tap of the day is used.
-- 3. Alighting stop: among later stops on the same route direction that the
-- boarding vehicle reached within two hours, the one closest to the next
-- boarding stop. alighting_within_walk_cap flags whether that distance is
-- plausible on foot.
-- gps_service_matches_transaction records whether the GPS feed reported the same
-- service as the ticketing registry.
--
-- confidence_tier is a heuristic label on each prediction: high when the walk is
-- short, the runner-up candidate is clearly farther and the next origin was an
-- observed later tap; low when the walk is long or the runner-up is about as
-- close; medium otherwise. The last journey of the day uses the morning's first
-- tap as next origin, a weaker anchor, so it is capped at medium. Thresholds are
-- the Jinja constants above and should be recalibrated against survey results.
--
-- Next origin fallback: when the next tap has no boarding stop (BRT, VLT, van,
-- metro, or an unmatched bus tap) its tap_location from ticketing stands in as
-- the next boarding location: the tile centroid for the sample source, the
-- validator coordinates for the Jaé source. next_boarding_location_source
-- records which was used.
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

{% set max_boarding_gap_s = 600 %}
{% set max_walk_m = 1000 %}
-- Confidence tier thresholds (metres). Stop options are ~290 m apart, so a margin
-- over the runner-up of 150 m means the best stop is clearly closer than its
-- neighbour, and under 50 m means the choice is effectively a coin toss.
{% set walk_high_m = 300 %}
{% set walk_low_m = 700 %}
{% set margin_high_m = 150 %}
{% set margin_low_m = 50 %}

with
    ticketing as (
        select
            rider_id,
            trip_sequence,
            service_date,
            boarding_time,
            vehicle_id as boarding_vehicle_id,
            route_id,
            servico,
            direction_id as transaction_direction_id,
            chain_position,
            tap_location
        from {{ ref("ticketing") }}
        where service_date = "{{ var('run_date') }}"
    ),

    -- CTE 1: Boarding stop, the vehicle's stop visit closest in time to the tap
    boarding as (
        select
            t.*,
            se.direction_id,
            case
                when se.direction_id = t.transaction_direction_id
                then 'transaction'
                when se.direction_id is not null
                then 'gps_inferred'
            end as direction_source,
            se.service as gps_service,
            se.stop_id as boarding_stop_id,
            se.stop_sequence as boarding_stop_sequence,
            se.arrival_time as boarding_stop_time,
            se.geography as boarding_stop_geography,
            case
                when t.boarding_time between se.arrival_time and se.departure_time
                then 0
                else
                    least(
                        abs(time_diff(t.boarding_time, se.arrival_time, second)),
                        abs(time_diff(t.boarding_time, se.departure_time, second))
                    )
            end as boarding_gap_s
        from ticketing as t
        left join
            {{ ref("stop_events") }} as se
            on se.service_date = t.service_date
            and right(se.vehicle_id, 5) = t.boarding_vehicle_id
            and se.route_id = t.route_id
            and se.is_forward
            and se.departure_time
            >= time_sub(t.boarding_time, interval {{ max_boarding_gap_s }} second)
            and se.arrival_time
            <= time_add(t.boarding_time, interval {{ max_boarding_gap_s }} second)
        qualify
            row_number() over (
                partition by t.service_date, t.rider_id, t.trip_sequence
                order by
                    se.direction_id = t.transaction_direction_id desc,
                    boarding_gap_s,
                    se.arrival_time
            )
            = 1
    ),

    -- CTE 2: Trip chaining, the next tap's boarding stop
    chained as (
        select
            *,
            case
                when chain_position != 'last'
                then
                    lead(boarding_time) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
                else
                    first_value(boarding_time) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
            end as next_boarding_time,
            case
                when chain_position != 'last'
                then
                    lead(boarding_vehicle_id) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
                else
                    first_value(boarding_vehicle_id) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
            end as next_boarding_vehicle_id,
            case
                when chain_position != 'last'
                then
                    lead(boarding_stop_id) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
                else
                    first_value(boarding_stop_id) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
            end as next_boarding_stop_id,
            case
                when chain_position != 'last'
                then
                    lead(boarding_stop_geography) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
                else
                    first_value(boarding_stop_geography) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
            end as next_boarding_stop_geography,
            case
                when chain_position != 'last'
                then
                    lead(tap_location) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
                else
                    first_value(tap_location) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
            end as next_tap_location
        from boarding
    ),

    -- Next boarding location: the next tap's stop when known, else its tap point
    with_next_location as (
        select
            *,
            coalesce(
                next_boarding_stop_geography, next_tap_location
            ) as next_boarding_location,
            case
                when next_boarding_stop_geography is not null
                then 'stop'
                when next_tap_location is not null
                then 'tap'
            end as next_boarding_location_source
        from chained
    ),

    -- CTE 3: Candidate alighting stops, later stops on the same route direction
    -- reached by the boarding vehicle within two hours. Single-tap days and taps
    -- with no next boarding location get none.
    candidates as (
        select
            c.*,
            se.stop_id as alighting_stop_id,
            se.stop_sequence as alighting_stop_sequence,
            se.arrival_time as alighting_time,
            se.geography as alighting_stop_geography,
            st_distance(
                se.geography, c.next_boarding_location
            ) as alighting_to_next_boarding_distance_m
        from with_next_location as c
        left join
            {{ ref("stop_events") }} as se
            on se.service_date = c.service_date
            and right(se.vehicle_id, 5) = c.boarding_vehicle_id
            and se.route_id = c.route_id
            and se.direction_id = c.direction_id
            and se.is_forward
            and se.stop_sequence > c.boarding_stop_sequence
            and se.arrival_time > c.boarding_stop_time
            and se.arrival_time <= time_add(c.boarding_time, interval 2 hour)
            and c.chain_position != 'only'
            -- No alighting can be scored without a next boarding location
            and c.next_boarding_location is not null
        -- One row per stop: a stop the vehicle visited twice within the window
        -- (e.g. on a second run) keeps its earliest visit, so the runner-up is
        -- always a different stop and n_candidate_stops counts stops, not visits
        qualify
            row_number() over (
                partition by c.service_date, c.rider_id, c.trip_sequence, se.stop_id
                order by se.arrival_time
            )
            = 1
    ),

    ranked as (
        select
            *,
            count(alighting_stop_id) over (
                partition by service_date, rider_id, trip_sequence
            ) as n_candidate_stops,
            row_number() over (
                partition by service_date, rider_id, trip_sequence
                order by
                    alighting_to_next_boarding_distance_m asc nulls last, alighting_time
            ) as candidate_rank,
            lead(alighting_to_next_boarding_distance_m) over (
                partition by service_date, rider_id, trip_sequence
                order by
                    alighting_to_next_boarding_distance_m asc nulls last, alighting_time
            ) as runner_up_walk_distance_m
        from candidates
    )

select
    rider_id,
    service_date,
    trip_sequence,
    chain_position,
    boarding_time,
    boarding_vehicle_id,
    route_id,
    servico,
    transaction_direction_id,
    direction_id,
    direction_source,
    gps_service,
    gps_service = servico as gps_service_matches_transaction,
    boarding_stop_id,
    boarding_stop_sequence,
    boarding_stop_time,
    boarding_gap_s,
    next_boarding_time,
    next_boarding_vehicle_id,
    next_boarding_stop_id,
    next_boarding_location_source,
    next_boarding_location,
    alighting_stop_id,
    alighting_stop_sequence,
    alighting_time,
    alighting_to_next_boarding_distance_m,
    st_distance(
        boarding_stop_geography, alighting_stop_geography
    ) as boarding_to_alighting_distance_m,
    n_candidate_stops,
    runner_up_walk_distance_m,
    alighting_to_next_boarding_distance_m
    <= {{ max_walk_m }} as alighting_within_walk_cap,
    case
        when alighting_stop_id is null
        then null
        when
            alighting_to_next_boarding_distance_m > {{ walk_low_m }}
            or runner_up_walk_distance_m - alighting_to_next_boarding_distance_m
            < {{ margin_low_m }}
        then 'low'
        when
            alighting_to_next_boarding_distance_m <= {{ walk_high_m }}
            and runner_up_walk_distance_m - alighting_to_next_boarding_distance_m
            >= {{ margin_high_m }}
            and chain_position != 'last'
        then 'high'
        else 'medium'
    end as confidence_tier
from ranked
where candidate_rank = 1
