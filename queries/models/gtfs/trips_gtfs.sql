{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["trip_id", "feed_start_date"],
        alias="trips",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(t.data_versao as date) as feed_start_date,
    fi.feed_end_date,
    safe_cast(json_value(t.content, '$.route_id') as string) route_id,
    safe_cast(json_value(t.content, '$.service_id') as string) service_id,
    safe_cast(t.trip_id as string) trip_id,
    safe_cast(json_value(t.content, '$.trip_headsign') as string) trip_headsign,
    safe_cast(json_value(t.content, '$.trip_short_name') as string) trip_short_name,
    regexp_replace(
        safe_cast(json_value(t.content, '$.direction_id') as string), r'\.0$', ''
    ) as direction_id,
    safe_cast(json_value(t.content, '$.block_id') as string) block_id,
    safe_cast(json_value(t.content, '$.shape_id') as string) shape_id,
    safe_cast(
        json_value(t.content, '$.wheelchair_accessible') as string
    ) wheelchair_accessible,
    safe_cast(json_value(t.content, '$.bikes_allowed') as string) bikes_allowed,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "trips") }} t
join
    {{ ref("feed_info_gtfs") }} fi on t.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        t.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
