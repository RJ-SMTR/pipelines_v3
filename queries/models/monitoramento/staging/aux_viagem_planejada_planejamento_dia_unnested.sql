{{ config(materialized="ephemeral") }}

with
    viagem_planejada as (
        select
            data,
            feed_start_date,
            servico,
            vista,
            route_id,
            trip_id,
            shape_id,
            trajetos_alternativos
        from {{ ref("viagem_planejada_planejamento_dia") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
    ),
    trajetos as (
        select data, feed_start_date, servico, vista, route_id, trip_id, shape_id
        from viagem_planejada

        union all

        select
            vp.data,
            vp.feed_start_date,
            vp.servico,
            alt.vista,
            vp.route_id,
            alt.trip_id,
            alt.shape_id
        from viagem_planejada as vp
        cross join unnest(vp.trajetos_alternativos) as alt
    )
select distinct data, feed_start_date, servico, vista, route_id, trip_id, shape_id
from trajetos
