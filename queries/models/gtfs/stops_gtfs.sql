{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["stop_id", "feed_start_date"],
        alias="stops",
        tags=["geolocalizacao"],
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(s.data_versao as date) as feed_start_date,
    fi.feed_end_date,
    safe_cast(s.stop_id as string) stop_id,
    safe_cast(json_value(s.content, '$.stop_code') as string) stop_code,
    safe_cast(json_value(s.content, '$.stop_name') as string) stop_name,
    safe_cast(json_value(s.content, '$.tts_stop_name') as string) tts_stop_name,
    safe_cast(json_value(s.content, '$.stop_desc') as string) stop_desc,
    safe_cast(json_value(s.content, '$.stop_lat') as float64) stop_lat,
    safe_cast(json_value(s.content, '$.stop_lon') as float64) stop_lon,
    safe_cast(json_value(s.content, '$.zone_id') as string) zone_id,
    safe_cast(json_value(s.content, '$.stop_url') as string) stop_url,
    safe_cast(
        safe_cast(
            safe_cast(json_value(s.content, '$.location_type') as float64) as int64
        ) as string
    ) location_type,
    safe_cast(json_value(s.content, '$.parent_station') as string) parent_station,
    safe_cast(json_value(s.content, '$.stop_timezone') as string) stop_timezone,
    safe_cast(
        json_value(s.content, '$.wheelchair_boarding') as string
    ) wheelchair_boarding,
    safe_cast(json_value(s.content, '$.level_id') as string) level_id,
    safe_cast(json_value(s.content, '$.platform_code') as string) platform_code,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "stops") }} s
join
    {{ ref("feed_info_gtfs") }} fi on s.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        s.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
