with
    n_passengers as (
        select
            h3_gps.service_date,
            h3_gps.vehicle_id,
            h3_gps.service,
            h3_gps.mode,
            h3_gps.tile_id,
            h3_gps.tile_entry_time,
            h3_gps.tile_exit_time,
            count(*) - 1 as n

        from {{ ref("h3_gps") }} as h3_gps

        left join
            {{ ref("origin_destination") }} as origin_destination
            on h3_gps.service_date = origin_destination.service_date  -- Same date
            and right(h3_gps.vehicle_id, 5) = origin_destination.boarding_vehicle_id  -- Same vehicle
            -- On bus cases:
            -- 1. Getting on (transaction time between tile time)
            -- 2. Getting off (alighting_tile_entry_time = tile_entry_time)
            -- 3. Riding (tile time is between transaction time and destination time)
            -- CASE 1
            and (
                origin_destination.boarding_time
                between h3_gps.tile_entry_time and h3_gps.tile_exit_time
                -- CASE 2
                or origin_destination.alighting_tile_entry_time = h3_gps.tile_entry_time
                -- CASE 3
                or h3_gps.tile_entry_time
                between origin_destination.boarding_time
                and origin_destination.alighting_tile_entry_time
            )
        -- AND (
        -- alighting_tile_entry_time <= next_boarding_time
        -- OR chain_position IN ('last', 'only')
        -- )
        where h3_gps.service_date between '2023-03-01' and '2023-06-30'
        -- AND alighting_to_next_boarding_distance_m <= 2000
        -- AND h3_gps.vehicle_id = 'A41190'
        group by
            h3_gps.service_date,
            h3_gps.vehicle_id,
            h3_gps.service,
            h3_gps.mode,
            h3_gps.tile_id,
            h3_gps.tile_entry_time,
            h3_gps.tile_exit_time
    ),

    capacity as (
        select
            n_passengers.service_date,
            n_passengers.vehicle_id,
            service,
            n_passengers.mode,
            tile_id,
            tile_entry_time,
            tile_exit_time,
            n,
            nullif(passengers_sitting, 0) as capacity_sitting,
            nullif(passengers_standing, 0) as capacity_standing,
            nullif(passengers_sitting + passengers_standing, 0) as capacity_total

        from n_passengers

        left join
            (
                select *
                from {{ ref("vw_vehicle_details") }}
                where latest_capture = 'TRUE'
            ) as vw_vehicle_details
            on n_passengers.vehicle_id = vw_vehicle_details.vehicle_id
    ),

    capacity_percentages as (
        select
            *,
            round(n / capacity_sitting, 2) as p_sitting_utilization,
            round(n / capacity_total, 2) as p_utilization
        from capacity
    )

select
    utilisation_observations.int64_field_0,
    utilisation_observations.time,
    cp.service_date,
    cp.vehicle_id,
    cp.tile_id,
    cp.n,
    cp.p_sitting_utilization,
    cp.p_utilization,
    stop_lat,
    stop_lon,
    centroid,
    st_distance(centroid, st_geogpoint(stop_lon, stop_lat)) as distance,
    utilisation,
    utilisation_observations.line,
    utilisation_observations.researcher,
    utilisation_observations.location

from capacity_percentages as cp
inner join
    `rj-smtr-dev`.mit_ipea_project.utilisation_observations
    on right(cp.vehicle_id, 5) = utilisation_observations.vehicle_id  -- Same Vehicle
    and cp.service_date = utilisation_observations.date  -- Same date
    and extract(
        time
        from datetime
    ) between time_sub(tile_entry_time, interval 5 minute) and time_add(
        tile_exit_time, interval 5 minute
    )  -- Obs time match

left join {{ ref("vw_h3") }} as vw_h3 on cp.tile_id = vw_h3.tile_id

order by int64_field_0
