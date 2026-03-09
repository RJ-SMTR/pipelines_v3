{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="shapes_geom",
        tags=["geolocalizacao"],
    )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

with
    shapes as (
        select
            feed_version,
            feed_start_date,
            feed_end_date,
            shape_id,
            shape_pt_sequence,
            st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
            concat(shape_pt_lon, " ", shape_pt_lat) as lon_lat,
        from {{ ref("shapes_gtfs") }}
        {# from `rj-smtr.gtfs.shapes` #}
        {% if is_incremental() %}
            where
                feed_start_date
                in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        {% endif %}
    ),
    shapes_agg as (
        select
            feed_start_date,
            feed_end_date,
            feed_version,
            shape_id,
            array_agg(ponto_shape order by shape_pt_sequence) as array_shape,
            concat(
                "LINESTRING(", string_agg(lon_lat, ", " order by shape_pt_sequence), ")"
            ) as wkt_shape

        from shapes
        group by 1, 2, 3, 4
    )
select
    feed_start_date,
    feed_end_date,
    feed_version,
    shape_id,
    st_makeline(array_shape) as shape,
    wkt_shape,
    array_shape[ordinal(1)] as start_pt,
    array_shape[ordinal(array_length(array_shape))] as end_pt,
    '{{ var("version") }}' as versao
from shapes_agg
