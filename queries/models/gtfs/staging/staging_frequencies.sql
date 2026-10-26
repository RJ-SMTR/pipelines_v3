{{ config(materialized="view", schema="gtfs_staging", alias="frequencies") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(trip_id as string) as trip_id,
    safe_cast(start_time as string) as start_time,
    safe_cast(json_value(content, '$.end_time') as string) as end_time,
    safe_cast(json_value(content, '$.headway_secs') as string) as headway_secs,
    safe_cast(json_value(content, '$.exact_times') as string) as exact_times,
    timestamp_captura
from {{ source("source_gtfs", "frequencies") }}
