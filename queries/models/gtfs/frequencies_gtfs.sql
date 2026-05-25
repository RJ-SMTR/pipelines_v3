{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["trip_id", "start_time", "feed_start_date"],
        alias="frequencies",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(f.data_versao as date) as feed_start_date,
    fi.feed_end_date,
    safe_cast(f.trip_id as string) trip_id,
    safe_cast(f.start_time as string) start_time,
    safe_cast(json_value(f.content, '$.end_time') as string) end_time,
    safe_cast(json_value(f.content, '$.headway_secs') as int64) headway_secs,
    safe_cast(json_value(f.content, '$.exact_times') as string) exact_times,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "frequencies") }} f
join
    {{ ref("feed_info_gtfs") }} fi on f.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        f.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
