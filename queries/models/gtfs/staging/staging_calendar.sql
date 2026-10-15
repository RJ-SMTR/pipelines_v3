{{ config(materialized="view", schema="gtfs_staging", alias="calendar") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(service_id as string) as service_id,
    safe_cast(json_value(content, '$.monday') as string) as monday,
    safe_cast(json_value(content, '$.tuesday') as string) as tuesday,
    safe_cast(json_value(content, '$.wednesday') as string) as wednesday,
    safe_cast(json_value(content, '$.thursday') as string) as thursday,
    safe_cast(json_value(content, '$.friday') as string) as friday,
    safe_cast(json_value(content, '$.saturday') as string) as saturday,
    safe_cast(json_value(content, '$.sunday') as string) as sunday,
    safe_cast(json_value(content, '$.start_date') as string) as start_date,
    safe_cast(json_value(content, '$.end_date') as string) as end_date,
    timestamp_captura
from {{ source("source_gtfs", "calendar") }}
