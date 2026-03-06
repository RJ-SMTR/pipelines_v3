{{ config(materialized="ephemeral") }}

with
    stop_times as (
        select *, split(arrival_time, ":") as arrival_time_parts,
        {# from `rj-smtr.gtfs.stop_times` #}
        from {{ ref("stop_times_gtfs") }}
    )

select
    * except (arrival_time, arrival_time_parts),
    make_interval(
        hour => cast(arrival_time_parts[0] as integer),
        minute => cast(arrival_time_parts[1] as integer),
        second => cast(arrival_time_parts[2] as integer)
    ) as arrival_time
from stop_times
