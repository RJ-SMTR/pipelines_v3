{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["shape_id", "shape_pt_sequence", "feed_start_date"],
        alias="shapes",
        tags=["geolocalizacao"],
    )
}}

{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

select
    fi.feed_version,
    safe_cast(s.data_versao as date) as feed_start_date,
    fi.feed_end_date,
    safe_cast(s.shape_id as string) shape_id,
    safe_cast(json_value(s.content, '$.shape_pt_lat') as float64) shape_pt_lat,
    safe_cast(json_value(s.content, '$.shape_pt_lon') as float64) shape_pt_lon,
    safe_cast(s.shape_pt_sequence as int64) shape_pt_sequence,
    safe_cast(
        json_value(s.content, '$.shape_dist_traveled') as float64
    ) shape_dist_traveled,
    '{{ var("version") }}' as versao_modelo
from {{ source("br_rj_riodejaneiro_gtfs_staging", "shapes") }} s
join
    {{ ref("feed_info_gtfs") }} fi on s.data_versao = cast(fi.feed_start_date as string)
{% if is_incremental() -%}
    where
        s.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{%- endif %}
