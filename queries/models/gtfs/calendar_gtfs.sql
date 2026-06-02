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
    safe_cast(c.data_versao as date) feed_start_date,
    fi.feed_end_date,
    safe_cast(c.service_id as string) service_id,
    safe_cast(json_value(c.content, '$.monday') as string) monday,
    safe_cast(json_value(c.content, '$.tuesday') as string) tuesday,
    safe_cast(json_value(c.content, '$.wednesday') as string) wednesday,
    safe_cast(json_value(c.content, '$.thursday') as string) thursday,
    safe_cast(json_value(c.content, '$.friday') as string) friday,
    safe_cast(json_value(c.content, '$.saturday') as string) saturday,
    safe_cast(json_value(c.content, '$.sunday') as string) sunday,
    parse_date(
        '%Y%m%d', safe_cast(json_value(c.content, '$.start_date') as string)
    ) start_date,
    parse_date(
        '%Y%m%d', safe_cast(json_value(c.content, '$.end_date') as string)
    ) end_date,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "calendar") }} c
join
    {{ ref("feed_info_gtfs") }} fi on c.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        c.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
