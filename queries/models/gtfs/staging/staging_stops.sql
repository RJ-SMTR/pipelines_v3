{{ config(materialized="view", schema="gtfs_staging", alias="stops") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(stop_id as string) as stop_id,
    safe_cast(json_value(content, '$.stop_code') as string) as stop_code,
    safe_cast(json_value(content, '$.stop_name') as string) as stop_name,
    safe_cast(json_value(content, '$.tts_stop_name') as string) as tts_stop_name,
    safe_cast(json_value(content, '$.stop_desc') as string) as stop_desc,
    safe_cast(json_value(content, '$.stop_lat') as string) as stop_lat,
    safe_cast(json_value(content, '$.stop_lon') as string) as stop_lon,
    safe_cast(json_value(content, '$.zone_id') as string) as zone_id,
    safe_cast(json_value(content, '$.stop_url') as string) as stop_url,
    safe_cast(json_value(content, '$.location_type') as string) as location_type,
    safe_cast(json_value(content, '$.parent_station') as string) as parent_station,
    safe_cast(json_value(content, '$.stop_timezone') as string) as stop_timezone,
    safe_cast(
        json_value(content, '$.wheelchair_boarding') as string
    ) as wheelchair_boarding,
    safe_cast(json_value(content, '$.level_id') as string) as level_id,
    safe_cast(json_value(content, '$.platform_code') as string) as platform_code,
    timestamp_captura
from {{ source("source_gtfs", "stops") }}
