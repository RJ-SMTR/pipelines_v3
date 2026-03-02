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
    make_interval(
        hour => cast(start_time_parts[0] as integer),
        minute => cast(start_time_parts[1] as integer),
        second => cast(start_time_parts[2] as integer)
    ) as start_time,
    make_interval(
        hour => cast(end_time_parts[0] as integer),
        minute => cast(end_time_parts[1] as integer),
        second => cast(end_time_parts[2] as integer)
    ) as end_time
from frequencies
