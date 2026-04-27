{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["fare_id", "feed_start_date"],
        alias="fare_rules",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(fr.data_versao as date) feed_start_date,
    fi.feed_end_date,
    safe_cast(fr.fare_id as string) fare_id,
    safe_cast(fr.route_id as string) route_id,
    safe_cast(json_value(fr.content, '$.origin_id') as string) origin_id,
    safe_cast(json_value(fr.content, '$.destination_id') as string) destination_id,
    safe_cast(json_value(fr.content, '$.contains_id') as string) contains_id,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "fare_rules") }} fr
join
    {{ ref("feed_info_gtfs") }} fi
    on fr.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        fr.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
