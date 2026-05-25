{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["trip_id", "feed_start_date"],
        alias="stop_times",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(st.data_versao as date) as feed_start_date,
    fi.feed_end_date,
    safe_cast(st.trip_id as string) trip_id,
    safe_cast(json_value(st.content, '$.arrival_time') as string) arrival_time,
    safe_cast(json_value(st.content, '$.departure_time') as datetime) departure_time,
    safe_cast(json_value(st.content, '$.stop_id') as string) stop_id,
    safe_cast(st.stop_sequence as int64) stop_sequence,
    safe_cast(json_value(st.content, '$.stop_headsign') as string) stop_headsign,
    safe_cast(json_value(st.content, '$.pickup_type') as string) pickup_type,
    safe_cast(json_value(st.content, '$.drop_off_type') as string) drop_off_type,
    safe_cast(
        json_value(st.content, '$.continuous_pickup') as string
    ) continuous_pickup,
    safe_cast(
        json_value(st.content, '$.continuous_drop_off') as string
    ) continuous_drop_off,
    safe_cast(
        json_value(st.content, '$.shape_dist_traveled') as float64
    ) shape_dist_traveled,
    safe_cast(json_value(st.content, '$.timepoint') as string) timepoint,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "stop_times") }} st
join
    {{ ref("feed_info_gtfs") }} fi
    on st.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        st.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
