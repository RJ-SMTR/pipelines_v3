{{ config(materialized="view", schema="gtfs_staging", alias="stop_times") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(trip_id as string) as trip_id,
    safe_cast(json_value(content, '$.arrival_time') as string) as arrival_time,
    safe_cast(json_value(content, '$.departure_time') as string) as departure_time,
    safe_cast(json_value(content, '$.stop_id') as string) as stop_id,
    safe_cast(stop_sequence as string) as stop_sequence,
    safe_cast(json_value(content, '$.stop_headsign') as string) as stop_headsign,
    safe_cast(json_value(content, '$.pickup_type') as string) as pickup_type,
    safe_cast(json_value(content, '$.drop_off_type') as string) as drop_off_type,
    safe_cast(
        json_value(content, '$.continuous_pickup') as string
    ) as continuous_pickup,
    safe_cast(
        json_value(content, '$.continuous_drop_off') as string
    ) as continuous_drop_off,
    safe_cast(
        json_value(content, '$.shape_dist_traveled') as string
    ) as shape_dist_traveled,
    safe_cast(json_value(content, '$.timepoint') as string) as timepoint,
    timestamp_captura
from {{ source("source_gtfs", "stop_times") }}
