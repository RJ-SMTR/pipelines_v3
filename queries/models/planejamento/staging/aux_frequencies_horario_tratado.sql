{{ config(materialized="ephemeral") }}

with
    frequencies as (
        select
            *,
            split(start_time, ":") as start_time_parts,
            split(end_time, ":") as end_time_parts,
        {# from `rj-smtr.gtfs.frequencies` #}
        from {{ ref("frequencies_gtfs") }}
    )

select
    * except (start_time_parts, end_time_parts, start_time, end_time),
    cast(start_time_parts[0] as int64) * 3600
    + cast(start_time_parts[1] as int64) * 60
    + cast(start_time_parts[2] as int64) as start_seconds,
    cast(end_time_parts[0] as int64) * 3600
    + cast(end_time_parts[1] as int64) * 60
    + cast(end_time_parts[2] as int64) as end_seconds
from frequencies
