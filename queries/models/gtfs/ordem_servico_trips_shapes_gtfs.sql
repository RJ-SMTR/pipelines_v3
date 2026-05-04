{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=[
            "feed_start_date",
            "tipo_dia",
            "tipo_os",
            "servico",
            "sentido",
            "faixa_horaria_inicio",
            "shape_id",
        ],
        alias="ordem_servico_trips_shapes",
    )
}}

with
    ordem_servico_trips_shapes_gtfs as (
        select *
        from {{ ref("ordem_servico_trips_shapes_gtfs_v1") }}
        where
            feed_start_date < date("{{ var('DATA_GTFS_V4_INICIO') }}")
        --fmt:off
        full outer union all by name
        --fmt:on
        select *
        from {{ ref("ordem_servico_trips_shapes_gtfs_v2") }}
        where feed_start_date >= date("{{ var('DATA_GTFS_V4_INICIO') }}")
    )

select *, '{{ invocation_id }}' as id_execucao_dbt
from ordem_servico_trips_shapes_gtfs
