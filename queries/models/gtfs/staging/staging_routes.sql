{{ config(materialized="view", schema="gtfs_staging", alias="routes") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(route_id as string) as route_id,
    safe_cast(json_value(content, '$.agency_id') as string) as agency_id,
    safe_cast(json_value(content, '$.route_short_name') as string) as route_short_name,
    safe_cast(json_value(content, '$.route_long_name') as string) as route_long_name,
    safe_cast(json_value(content, '$.route_desc') as string) as route_desc,
    safe_cast(json_value(content, '$.route_type') as string) as route_type,
    safe_cast(json_value(content, '$.route_url') as string) as route_url,
    safe_cast(json_value(content, '$.route_color') as string) as route_color,
    safe_cast(json_value(content, '$.route_text_color') as string) as route_text_color,
    safe_cast(json_value(content, '$.route_sort_order') as string) as route_sort_order,
    safe_cast(
        json_value(content, '$.continuous_pickup') as string
    ) as continuous_pickup,
    safe_cast(
        json_value(content, '$.continuous_drop_off') as string
    ) as continuous_drop_off,
    safe_cast(json_value(content, '$.network_id') as string) as network_id,
    timestamp_captura
from {{ source("source_gtfs", "routes") }}
