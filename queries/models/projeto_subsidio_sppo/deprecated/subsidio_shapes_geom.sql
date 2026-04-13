-- depends_on: {{ ref('subsidio_data_versao_efetiva') }}
{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_versao",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["data_versao", "shape_id"],
        incremental_strategy="insert_overwrite",
    )
}}
{% if is_incremental() and execute %}
    {% set run_date_str = "'" ~ var("run_date") ~ "'" %}
    {% set query = (
        "SELECT COALESCE(data_versao_shapes, feed_start_date) FROM "
        ~ ref("subsidio_data_versao_efetiva")
        ~ " WHERE data BETWEEN DATE_SUB(DATE("
        ~ run_date_str
        ~ "), INTERVAL 1 DAY) AND DATE("
        ~ run_date_str
        ~ ")"
    ) %}
    {% set result = run_query(query) %}
    {% set data_versao_shapes = result.columns[0].values() %}
{% endif %}
with
    contents as (
        select
            shape_id,
            st_geogpoint(
                safe_cast(shape_pt_lon as float64), safe_cast(shape_pt_lat as float64)
            ) ponto_shape,
            safe_cast(shape_pt_sequence as int64) shape_pt_sequence,
            date(data_versao) as data_versao
        from {{ var("subsidio_shapes") }} s
        {% if is_incremental() %}
            where data_versao in ("{{ data_versao_shapes | join('", "') }}")
        {% endif %}
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
        -- BUILD LINESTRINGS OVER SHAPE POINTS
        select shape_id, data_versao, st_makeline(array_agg(ponto_shape)) as shape
        from pts
        group by 1, 2
    ),
    boundary as (
        -- EXTRACT START AND END POINTS FROM SHAPES
        select
            c1.shape_id, c1.ponto_shape start_pt, c2.ponto_shape end_pt, c1.data_versao
        from (select * from pts where shape_pt_sequence = 1) c1
        join
            (select * from pts where shape_pt_sequence = final_pt_sequence) c2
            on c1.shape_id = c2.shape_id
            and c1.data_versao = c2.data_versao
    ),
    merged as (
        -- JOIN SHAPES AND BOUNDARY POINTS
        select
            s.*,
            b.* except (data_versao, shape_id),
            round(st_length(shape), 1) shape_distance,
        from shapes s
        join boundary b on s.shape_id = b.shape_id and s.data_versao = b.data_versao
    ),
    ids as (
        select
            shape_id,
            shape,
            shape_distance,
            start_pt,
            end_pt,
            data_versao,
            row_number() over (partition by data_versao, shape_id) rn
        from merged m
    )
select * except (rn), '{{ var("version") }}' as versao_modelo
from ids
where rn = 1
