-- depends_on: {{ ref('feed_info_gtfs') }}
{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["shape_id", "feed_start_date"],
        alias="shapes_geom",
        tags=["geolocalizacao"],
    )
}}

{% if execute %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

{% set shapes_gtfs = ref("shapes_gtfs") %}
{% set feed_info_gtfs = ref("feed_info_gtfs") %}
{# {% set feed_info_gtfs = "rj-smtr.gtfs.feed_info" %} #}
{# {% set shapes_gtfs = "rj-smtr.gtfs.shapes" %} #}
with
    contents as (
        select
            shape_id,
            st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
            shape_pt_sequence,
            feed_start_date,
        from {{ shapes_gtfs }} s
        where
            feed_start_date
            in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
    ),
    pts as (
        select
            *,
            max(shape_pt_sequence) over (
                partition by feed_start_date, shape_id
            ) final_pt_sequence
        from contents c
        order by feed_start_date, shape_id, shape_pt_sequence
    ),
    shapes as (
        -- BUILD LINESTRINGS OVER SHAPE POINTS
        select
            shape_id,
            feed_start_date,
            st_makeline(array_agg(ponto_shape)) as shape,
            array_agg(ponto_shape)[ordinal(1)] as start_pt,
            array_agg(ponto_shape)[
                ordinal(array_length(array_agg(ponto_shape)))
            ] as end_pt,
        from pts
        group by 1, 2
    ),
    shapes_half as (
        -- BUILD HALF LINESTRINGS OVER SHAPE POINTS
        (
            select
                shape_id,
                feed_start_date,
                shape_id || "_0" as new_shape_id,
                st_makeline(array_agg(ponto_shape)) as shape,
                array_agg(ponto_shape)[ordinal(1)] as start_pt,
                array_agg(ponto_shape)[
                    ordinal(array_length(array_agg(ponto_shape)))
                ] as end_pt,
            from pts
            where shape_pt_sequence <= round(final_pt_sequence / 2)
            group by 1, 2
        )
        union all
        (
            select
                shape_id,
                feed_start_date,
                shape_id || "_1" as new_shape_id,
                st_makeline(array_agg(ponto_shape)) as shape,
                array_agg(ponto_shape)[ordinal(1)] as start_pt,
                array_agg(ponto_shape)[
                    ordinal(array_length(array_agg(ponto_shape)))
                ] as end_pt,
            from pts
            where shape_pt_sequence > round(final_pt_sequence / 2)
            group by 1, 2
        )
    ),
    ids as (
        select * except (rn)
        from
            (
                select
                    feed_start_date,
                    shape_id,
                    shape,
                    start_pt,
                    end_pt,
                    row_number() over (partition by feed_start_date, shape_id) rn
                from shapes
            )
        where rn = 1
    ),
    union_shapes as (
        (select feed_start_date, shape_id, shape, start_pt, end_pt, from ids)
        union all
        (
            select
                feed_start_date,
                new_shape_id as shape_id,
                s.shape,
                s.start_pt,
                s.end_pt,
            from ids as i
            left join shapes_half as s using (feed_start_date, shape_id)
            where
                (
                    round(st_y(i.start_pt), 4) = round(st_y(i.end_pt), 4)
                    and round(st_x(i.start_pt), 4) = round(st_x(i.end_pt), 4)
                )
                or (
                    feed_start_date >= "{{ var('DATA_GTFS_V4_INICIO') }}"
                    and trunc(st_y(i.start_pt), 4) = trunc(st_y(i.end_pt), 4)
                    and trunc(st_x(i.start_pt), 4) = trunc(st_x(i.end_pt), 4)
                )
                or (
                    feed_start_date in ("2025-05-01", "2025-12-27")
                    and shape_id in ("iz18", "ycug")
                )  -- Operação Especial "Todo Mundo no Rio" - Lady Gaga e Reveillon 2025
        )
    )
select
    feed_version,
    feed_start_date,
    feed_end_date,
    shape_id,
    shape,
    round(st_length(shape), 1) shape_distance,
    start_pt,
    end_pt,
    '{{ var("version") }}' as versao_modelo
from union_shapes as m
left join {{ feed_info_gtfs }} as fi using (feed_start_date)
where fi.feed_start_date in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
