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

-- CTE 1: Match transaction with next transaction (if final transaction, match with
-- first transaction)
with
    ticketing as (
        select
            rider_id,
            trip_sequence,
            service_date,
            boarding_time as boarding_time,
            -- Predict destination time using trip chaining
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
            vehicle_id as boarding_vehicle_id,
            case
                when chain_position != 'last'
                then
                    lead(vehicle_id) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
                else
                    first_value(vehicle_id) over (
                        partition by service_date, rider_id order by trip_sequence
                    )
            end as next_boarding_vehicle_id,
            chain_position

        from {{ ref("ticketing") }}
        -- Test date for the sandbox sample; build h3_gps with run_date set to the
        -- same day
        where service_date = '2026-09-28'
    ),

    -- CTE 2: Estimate OD for each transaction
    origin_destination as (

        select
            rider_id,
            trip_sequence,
            ticketing.service_date,
            boarding_time,
            boarding_vehicle_id,
            h3_boarding.tile_id as boarding_tile_id,
            h3_boarding.centroid as boarding_tile_centroid,
            next_boarding_time,
            next_boarding_vehicle_id,
            h3_next_transaction.tile_id as next_boarding_tile_id,
            h3_next_transaction.centroid as next_boarding_tile_centroid,

            h3_bus_sitting.tile_entry_time as alighting_tile_entry_time,
            h3_bus_sitting.tile_exit_time as alighting_tile_exit_time,
            h3_bus_sitting.tile_id as alighting_tile_id,
            h3_bus_sitting.centroid as alighting_tile_centroid,
            chain_position,
            st_distance(
                h3_next_transaction.centroid, h3_bus_sitting.centroid
            ) as alighting_to_next_boarding_distance_m,
            min(
                st_distance(h3_next_transaction.centroid, h3_bus_sitting.centroid)
            ) over (partition by rider_id, ticketing.service_date, trip_sequence)
            as min_alighting_to_next_boarding_distance_m

        from ticketing

        -- NEXT TRANSACTION LOCATION
        left join
            {{ ref("h3_gps") }} as h3_next_transaction
            on ticketing.next_boarding_vehicle_id
            = right(h3_next_transaction.vehicle_id, 5)
            and ticketing.service_date = h3_next_transaction.service_date
            and ticketing.next_boarding_time >= h3_next_transaction.tile_entry_time
            and ticketing.next_boarding_time < h3_next_transaction.tile_exit_time
        -- BOARDING LOCATION
        left join
            {{ ref("h3_gps") }} as h3_boarding
            on ticketing.boarding_vehicle_id = right(h3_boarding.vehicle_id, 5)
            and ticketing.service_date = h3_boarding.service_date
            -- Between statement cannot be used for tile entry and exit times
            and ticketing.boarding_time >= h3_boarding.tile_entry_time
            and ticketing.boarding_time < h3_boarding.tile_exit_time
        -- SITTING ON BUS TIME
        left join
            {{ ref("h3_gps") }} as h3_bus_sitting
            on ticketing.boarding_vehicle_id = right(h3_bus_sitting.vehicle_id, 5)
            and ticketing.service_date = h3_bus_sitting.service_date
            and ticketing.boarding_time not between h3_bus_sitting.tile_entry_time
            and h3_bus_sitting.tile_exit_time
            and h3_bus_sitting.tile_entry_time
            between ticketing.boarding_time and time_add(
                ticketing.boarding_time, interval 2 hour
            )

    )

-- Output: UNION ALL
-- Transaction with more than one daily trip stage
select
    rider_id,
    service_date,
    boarding_time,
    chain_position,
    trip_sequence,
    boarding_vehicle_id,
    boarding_tile_id,
    min(alighting_tile_entry_time) as alighting_tile_entry_time,
    min(alighting_tile_exit_time) as alighting_tile_exit_time,
    alighting_tile_id,
    next_boarding_time,
    next_boarding_tile_id,
    avg(
        st_distance(boarding_tile_centroid, alighting_tile_centroid)
    ) as boarding_to_alighting_distance_m,
    alighting_to_next_boarding_distance_m
from origin_destination

where
    alighting_to_next_boarding_distance_m = min_alighting_to_next_boarding_distance_m
    and chain_position != 'only'

group by
    rider_id,
    service_date,
    boarding_time,
    chain_position,
    trip_sequence,
    boarding_vehicle_id,
    boarding_tile_id,
    alighting_tile_id,
    next_boarding_time,
    next_boarding_tile_id,
    alighting_to_next_boarding_distance_m

union all

-- Transaction with only 1 daily trip stage OR where there is no H3_GPS match
select distinct
    rider_id,
    service_date,
    boarding_time,
    chain_position,
    trip_sequence,
    boarding_vehicle_id,
    cast(null as string) as boarding_tile_id,
    cast(null as time) as alighting_tile_entry_time,
    cast(null as time) as alighting_tile_exit_time,
    cast(null as string) as alighting_tile_id,
    cast(null as time) as next_boarding_time,
    cast(null as string) as next_boarding_tile_id,
    null as boarding_to_alighting_distance_m,
    null as alighting_to_next_boarding_distance_m
from origin_destination
where chain_position = 'only' or alighting_to_next_boarding_distance_m is null
