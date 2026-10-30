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
        -- In future remove hardcoded date
        where service_date between '2023-03-01' and '2023-06-30'
    -- WHERE service_date IN (
    -- '2023-06-02'
    -- --, '2023-06-03'
    -- )
    ),

    -- CTE 2: Estimate OD for each transaction
    od_prediction as (

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
            cmatt30 as land_use_score,
            st_distance(
                h3_next_transaction.centroid, h3_bus_sitting.centroid
            ) as alighting_to_next_boarding_distance_m,
            min(
                st_distance(h3_next_transaction.centroid, h3_bus_sitting.centroid)
            ) over (partition by rider_id, ticketing.service_date, trip_sequence)
            as min_alighting_to_next_boarding_distance_m,
            row_number() over (
                partition by rider_id, ticketing.service_date, trip_sequence
                order by h3_bus_sitting.tile_entry_time
            ) as row_num

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
        left join
            `rj-smtr-dev`.mit_ipea_project.aop_walk_peak_2019_res9
            on h3_bus_sitting.tile_id = aop_walk_peak_2019_res9.id_hex

    ),

    min_exit_row as (
        -- May be multiple exit rows
        select
            *,
            case
                when
                    alighting_to_next_boarding_distance_m
                    = min_alighting_to_next_boarding_distance_m
                then 1
                else 0
            end as exit_row
        from od_prediction
    ),

    exit_row_num as (
        select
            *,
            min(case when exit_row = 1 then row_num end) over (
                partition by service_date, rider_id, trip_sequence
            ) as min_row_num_with_exit_row_1,
            min(case when exit_row = 1 then alighting_tile_entry_time end) over (
                partition by service_date, rider_id, trip_sequence
            ) as trip_chaining_destination_time,
            case
                when
                    time_diff(
                        alighting_tile_entry_time,
                        min(
                            case when exit_row = 1 then alighting_tile_entry_time end
                        ) over (partition by service_date, rider_id, trip_sequence),
                        minute
                    )
                    = 0
                then land_use_score
                else
                    land_use_score * (
                        1 / power(
                            abs(
                                time_diff(
                                    alighting_tile_entry_time,
                                    min(
                                        case
                                            when exit_row = 1
                                            then alighting_tile_entry_time
                                        end
                                    ) over (
                                        partition by
                                            service_date, rider_id, trip_sequence
                                    ),
                                    minute
                                )
                            ),
                            1
                        )
                    )
            end as land_use_scaled_score,
            abs(
                time_diff(
                    alighting_tile_entry_time,
                    min(
                        case when exit_row = 1 then alighting_tile_entry_time end
                    ) over (partition by service_date, rider_id, trip_sequence),
                    minute
                )
            ) as time_difference
        from min_exit_row
    ),

    land_use_time_estimate as (
        select
            *,
            case
                when
                    land_use_scaled_score = max(land_use_scaled_score) over (
                        partition by service_date, rider_id, trip_sequence
                    )
                then 1
                else 0
            end as exit_row_land_use_time
        from exit_row_num
        where
            row_num
            between min_row_num_with_exit_row_1 - 5 and min_row_num_with_exit_row_1 + 5
    -- AND rider_id IN
    -- ('000419a63840109bb1f656d75cd2c8f48f8ea90a4a72124f421ad8962094ad12'
    -- 'caf2a50e1ba90fde80690abcc9efc62ca1747702afa20761ecce65d68979b3fd',
    -- 'b76259e138c3d18c1f8e6e198ec91482dd257545a11c3af6addbd8d97d50dde8'
    -- )
    -- AND trip_sequence = 2
    -- AND service_date = '2023-04-12'
    ),

    land_use_row_num as (

        select
            *,
            min(case when exit_row_land_use_time = 1 then row_num end) over (
                partition by service_date, rider_id, trip_sequence
            ) as min_row_num_with_land_use_exit_row
        from land_use_time_estimate
    ),

    land_use_gravity as (
        select
            *,

            1 - (
                row_number() over (
                    partition by service_date, rider_id, trip_sequence order by row_num
                )
                / count(*) over (partition by service_date, rider_id, trip_sequence)
            ) as fraction_on_board,
            -- 1 - (row_num - min_row_num_with_exit_row_1 + 1) /
            -- (min_row_num_with_land_use_exit_row - min_row_num_with_exit_row_1 + 1)
            -- AS numerator,
            min_row_num_with_land_use_exit_row
            - min_row_num_with_exit_row_1
            + 1 as num_of_rows
        from land_use_row_num
        where
            (
                row_num >= min_row_num_with_exit_row_1
                and row_num <= min_row_num_with_land_use_exit_row
            )
            or (
                row_num <= min_row_num_with_exit_row_1
                and row_num >= min_row_num_with_land_use_exit_row
            )

    -- WHERE exit_row_land_use_time = 1 --xOR row_num = min_row_num_with_exit_row_1
    -- ORDER BY service_date, rider_id, trip_sequence, alighting_tile_entry_time
    ),

    origin_destination as (

        select
            rider_id,
            service_date,
            boarding_time,
            chain_position,
            trip_sequence,
            boarding_vehicle_id,
            boarding_tile_id,
            alighting_tile_entry_time as alighting_tile_entry_time,
            alighting_tile_exit_time as alighting_tile_exit_time,
            alighting_tile_id,
            next_boarding_time,
            next_boarding_tile_id,
            land_use_score,
            st_distance(
                boarding_tile_centroid, alighting_tile_centroid
            ) as boarding_to_alighting_distance_m,
            alighting_to_next_boarding_distance_m,
            min_alighting_to_next_boarding_distance_m as distance_transit,
            row_num,
            exit_row,
            exit_row_land_use_time,
            fraction_on_board
        from land_use_gravity

        where chain_position != 'only'
    -- AND exit_row_land_use_time = 1
    ),

    -- Output: UNION ALL
    -- Transaction with more than one daily trip stage
    output as (

        select
            rider_id,
            service_date,
            boarding_time,
            chain_position,
            trip_sequence,
            boarding_vehicle_id,
            boarding_tile_id,
            alighting_tile_entry_time,
            alighting_tile_exit_time,
            alighting_tile_id,
            next_boarding_time,
            next_boarding_tile_id,
            land_use_score,
            boarding_to_alighting_distance_m,
            alighting_to_next_boarding_distance_m,
            distance_transit,
            row_num,
            exit_row,
            exit_row_land_use_time,
            fraction_on_board
        from origin_destination

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
            null as land_use_score,
            null as boarding_to_alighting_distance_m,
            null as alighting_to_next_boarding_distance_m,
            null as distance_transit,
            null as row_num,
            null as exit_row,
            null as exit_row_land_use_time,
            null as fraction_on_board
        from od_prediction
        where chain_position = 'only' or alighting_to_next_boarding_distance_m is null
    )

select *
from
    output

    -- WHERE rider_id IN
    -- ('000419a63840109bb1f656d75cd2c8f48f8ea90a4a72124f421ad8962094ad12')
    -- AND trip_sequence = 2
    -- AND service_date = '2023-04-12'
    -- ORDER BY service_date, rider_id, trip_sequence, alighting_tile_entry_time
