{{ config(materialized="view", schema="gtfs_staging", alias="trips") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(json_value(content, '$.route_id') as string) as route_id,
    safe_cast(json_value(content, '$.service_id') as string) as service_id,
    safe_cast(trip_id as string) as trip_id,
    safe_cast(json_value(content, '$.trip_headsign') as string) as trip_headsign,
    safe_cast(json_value(content, '$.trip_short_name') as string) as trip_short_name,
    safe_cast(json_value(content, '$.direction_id') as string) as direction_id,
    safe_cast(json_value(content, '$.block_id') as string) as block_id,
    safe_cast(json_value(content, '$.shape_id') as string) as shape_id,
    safe_cast(
        json_value(content, '$.wheelchair_accessible') as string
    ) as wheelchair_accessible,
    safe_cast(json_value(content, '$.bikes_allowed') as string) as bikes_allowed,
    timestamp_captura
from {{ source("source_gtfs", "trips") }}
