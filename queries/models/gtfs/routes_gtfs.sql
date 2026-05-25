{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["route_id", "feed_start_date"],
        alias="routes",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(r.data_versao as date) feed_start_date,
    fi.feed_end_date,
    safe_cast(r.route_id as string) route_id,
    safe_cast(json_value(r.content, '$.agency_id') as string) agency_id,
    safe_cast(json_value(r.content, '$.route_short_name') as string) route_short_name,
    safe_cast(json_value(r.content, '$.route_long_name') as string) route_long_name,
    safe_cast(json_value(r.content, '$.route_desc') as string) route_desc,
    safe_cast(json_value(r.content, '$.route_type') as string) route_type,
    safe_cast(json_value(r.content, '$.route_url') as string) route_url,
    safe_cast(json_value(r.content, '$.route_color') as string) route_color,
    safe_cast(json_value(r.content, '$.route_text_color') as string) route_text_color,
    safe_cast(json_value(r.content, '$.route_sort_order') as int64) route_sort_order,
    safe_cast(json_value(r.content, '$.continuous_pickup') as string) continuous_pickup,
    safe_cast(
        json_value(r.content, '$.continuous_drop_off') as string
    ) continuous_drop_off,
    safe_cast(json_value(r.content, '$.network_id') as string) network_id,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "routes") }} r
join
    {{ ref("feed_info_gtfs") }} fi on r.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        r.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
