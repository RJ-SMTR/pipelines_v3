{{ config(materialized="view", schema="gtfs_staging", alias="shapes") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(shape_id as string) as shape_id,
    safe_cast(json_value(content, '$.shape_pt_lat') as string) as shape_pt_lat,
    safe_cast(json_value(content, '$.shape_pt_lon') as string) as shape_pt_lon,
    safe_cast(shape_pt_sequence as string) as shape_pt_sequence,
    safe_cast(
        json_value(content, '$.shape_dist_traveled') as string
    ) as shape_dist_traveled,
    timestamp_captura
from {{ source("source_gtfs", "shapes") }}
