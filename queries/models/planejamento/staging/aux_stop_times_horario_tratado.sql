{{ config(materialized="ephemeral") }}

with
    stop_times as (
        select *, split(arrival_time, ":") as arrival_time_parts,
        {# from `rj-smtr.gtfs.stop_times` #}
        from {{ ref("stop_times_gtfs") }}
    )

select
    * except (arrival_time, arrival_time_parts),
    cast(arrival_time_parts[0] as int64) * 3600
    + cast(arrival_time_parts[1] as int64) * 60
    + cast(arrival_time_parts[2] as int64) as arrival_seconds
from stop_times
