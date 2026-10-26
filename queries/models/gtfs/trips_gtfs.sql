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
    t.data_versao as feed_start_date,
    fi.feed_end_date,
    t.route_id,
    t.service_id,
    t.trip_id,
    t.trip_headsign,
    t.trip_short_name,
    regexp_replace(t.direction_id, r'\.0$', '') as direction_id,
    t.block_id,
    t.shape_id,
    t.wheelchair_accessible,
    t.bikes_allowed,
    '{{ var("version") }}' as versao_modelo
from {{ ref("staging_trips") }} t
join {{ ref("feed_info_gtfs") }} fi on t.data_versao = fi.feed_start_date
{% if is_incremental() -%}
    where
        t.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
