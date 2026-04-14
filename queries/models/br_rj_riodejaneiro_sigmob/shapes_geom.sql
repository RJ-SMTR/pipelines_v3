{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_versao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}
{% if is_incremental() and execute %}
    {% set start_date = (
        run_query("select max(data_versao) from " ~ this)
        .columns[0]
        .values()[0]
    ) %}
{% else %} {% set start_date = "2021-08-24" %}
{% endif %}
with
    trips as (
        select trip_id, shape_id, route_id, date(data_versao) data_versao
        from {{ ref("trips_desaninhada") }} t
        where
            date(data_versao) between date("{{start_date}}") and date_add(
                date("{{start_date}}"), interval 15 day
            )
    ),
    linhas as (
        select
            trip_id,
            shape_id,
            t.route_id,
            route_short_name linha,
            idmodalsmtr id_modal_smtr,
            t.data_versao,
        from trips t
        inner join
            (
                select *
                from {{ ref("routes_desaninhada") }}
                where
                    date(data_versao) between date("{{start_date}}") and date_add(
                        date("{{start_date}}"), interval 15 day
                    )
            ) r
            on t.route_id = r.route_id
            and t.data_versao = r.data_versao
    ),
    contents as (
        -- extracts values from json string field 'content'
        select
            shape_id,
            st_geogpoint(
                safe_cast(json_value(content, "$.shape_pt_lon") as float64),
                safe_cast(json_value(content, "$.shape_pt_lat") as float64)
            ) ponto_shape,
            safe_cast(
                json_value(content, "$.shape_pt_sequence") as int64
            ) shape_pt_sequence,
            date(data_versao) as data_versao
        from {{ ref("shapes") }} s
        where
            date(data_versao) between date("{{start_date}}") and date_add(
                date("{{start_date}}"), interval 15 day
            )
    ),
    pts as (
        select
            *,
            max(shape_pt_sequence) over (
                partition by data_versao, shape_id
            ) final_pt_sequence
        from contents c
        order by data_versao, shape_id, shape_pt_sequence
    ),
    shapes as (
        -- build linestrings over shape points
        select shape_id, data_versao, st_makeline(array_agg(ponto_shape)) as shape
        from pts
        group by 1, 2
    ),
    boundary as (
        -- extract start and end points from shapes
        select
            c1.shape_id, c1.ponto_shape start_pt, c2.ponto_shape end_pt, c1.data_versao
        from (select * from pts where shape_pt_sequence = 1) c1
        join
            (select * from pts where shape_pt_sequence = final_pt_sequence) c2
            on c1.shape_id = c2.shape_id
            and c1.data_versao = c2.data_versao
    ),
    merged as (
        -- join shapes and boundary points
        select
            s.*,
            b.* except (data_versao, shape_id),
            round(st_length(shape), 1) shape_distance,
        from shapes s
        join boundary b on s.shape_id = b.shape_id and s.data_versao = b.data_versao
    ),
    ids as (
        select
            trip_id,
            m.shape_id,
            route_id,
            id_modal_smtr,
            replace(linha, " ", "") as linha_gtfs,
            shape,
            shape_distance,
            start_pt,
            end_pt,
            m.data_versao,
            row_number() over (partition by m.data_versao, m.shape_id, l.trip_id) rn
        from merged m
        join linhas l on m.shape_id = l.shape_id and m.data_versao = l.data_versao
    -- mudar join para o route_id em todas as dependencias
    )
select * except (rn)
from ids
where
    rn = 1 {% if is_incremental %} and data_versao > date("{{start_date}}") {% endif %}
