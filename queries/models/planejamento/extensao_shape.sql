{{
    config(
        materialized="incremental",
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["feed_start_date", "shape_id"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    segmentos as (
        select
            feed_start_date,
            shape_id,
            cast(comprimento_segmento as numeric) as comprimento_segmento
        from {{ ref("aux_segmento_shape") }}
        {% if is_incremental() %}
            where feed_start_date = date('{{ var("data_versao_gtfs") }}')
        {% endif %}
    )
select
    feed_start_date,
    shape_id,
    sum(comprimento_segmento) / 1000 as extensao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ var("version") }}' as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from segmentos
group by 1, 2
