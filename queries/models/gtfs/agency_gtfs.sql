{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["agency_id", "feed_start_date"],
        alias="agency",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    a.data_versao feed_start_date,
    fi.feed_end_date,
    safe_cast(a.agency_id as string) agency_id,
    safe_cast(json_value(a.content, '$.agency_name') as string) agency_name,
    safe_cast(json_value(a.content, '$.agency_url') as string) agency_url,
    safe_cast(json_value(a.content, '$.agency_timezone') as string) agency_timezone,
    safe_cast(json_value(a.content, '$.agency_lang') as string) agency_lang,
    '{{ var("version") }}' as versao_modelo
from {{ source("source_gtfs", "agency") }} a
join {{ ref("feed_info_gtfs") }} fi on a.data_versao = fi.feed_start_date
{% if is_incremental() -%}
    where
        a.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
