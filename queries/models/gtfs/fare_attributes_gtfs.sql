{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["fare_id", "feed_start_date"],
        alias="fare_attributes",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    fa.data_versao feed_start_date,
    fi.feed_end_date,
    safe_cast(fa.fare_id as string) fare_id,
    safe_cast(json_value(fa.content, '$.price') as float64) price,
    safe_cast(json_value(fa.content, '$.currency_type') as string) currency_type,
    safe_cast(json_value(fa.content, '$.payment_method') as string) payment_method,
    safe_cast(json_value(fa.content, '$.transfers') as string) transfers,
    safe_cast(json_value(fa.content, '$.agency_id') as string) agency_id,
    safe_cast(json_value(fa.content, '$.transfer_duration') as int64) transfer_duration,
    '{{ var("version") }}' as versao_modelo
from {{ source("source_gtfs", "fare_attributes") }} fa
join {{ ref("feed_info_gtfs") }} fi on fa.data_versao = fi.feed_start_date
{% if is_incremental() -%}
    where
        fa.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
