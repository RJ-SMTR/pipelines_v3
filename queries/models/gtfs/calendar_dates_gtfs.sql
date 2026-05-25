{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["service_id", "date", "feed_start_date"],
        alias="calendar_dates",
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}


select
    fi.feed_version,
    safe_cast(cd.data_versao as date) feed_start_date,
    fi.feed_end_date,
    safe_cast(cd.service_id as string) service_id,
    parse_date('%Y%m%d', safe_cast(cd.date as string)) date,
    safe_cast(json_value(cd.content, '$.exception_type') as string) exception_type,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "calendar_dates") }} cd
join
    {{ ref("feed_info_gtfs") }} fi
    on cd.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        cd.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
