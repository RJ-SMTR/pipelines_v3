{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["service_id", "feed_start_date"],
        alias="calendar",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    c.data_versao feed_start_date,
    fi.feed_end_date,
    c.service_id,
    c.monday,
    c.tuesday,
    c.wednesday,
    c.thursday,
    c.friday,
    c.saturday,
    c.sunday,
    parse_date('%Y%m%d', c.start_date) start_date,
    parse_date('%Y%m%d', c.end_date) end_date,
    '{{ var("version") }}' as versao_modelo
from {{ ref("staging_calendar") }} c
join {{ ref("feed_info_gtfs") }} fi on c.data_versao = fi.feed_start_date
{% if is_incremental() -%}
    where
        c.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
