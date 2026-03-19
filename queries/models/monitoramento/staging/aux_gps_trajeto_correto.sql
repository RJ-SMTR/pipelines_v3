{{ config(materialized="ephemeral") }}

with
    registros as (
        select id_veiculo, servico, data, posicao_veiculo_geo, datetime_gps
        from {{ ref("aux_gps_filtrada") }}
    ),
    servico_planejado as (
        select data, feed_start_date, servico, sentido, trip_info, trajetos_alternativos
        from {{ ref("servico_planejado_faixa_horaria") }}
        {# from `rj-smtr.planejamento.servico_planejado_faixa_horaria` #}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
    ),
    servico_planejado_trip as (
        select sp.data, sp.feed_start_date, sp.servico, sp.sentido, trip.shape_id
        from servico_planejado sp, unnest(sp.trip_info) as trip
        where trip.shape_id is not null
    ),
    servico_planejado_trajetos_alternativos as (
        select
            sp.data,
            sp.feed_start_date,
            sp.servico,
            sp.sentido,
            trajetos_alternativos.shape_id
        from
            servico_planejado sp,
            unnest(sp.trajetos_alternativos) as trajetos_alternativos
        where trajetos_alternativos.shape_id is not null
    ),
    shape_union as (
        select distinct data, feed_start_date, servico, sentido, shape_id
        from
            (
                select data, feed_start_date, servico, sentido, shape_id
                from servico_planejado_trip
                union all
                select data, feed_start_date, servico, sentido, shape_id
                from servico_planejado_trajetos_alternativos
            )
    ),
    intersec as (
        select
            r.*,
            s.shape_id,
            st_dwithin(
                sg.shape, r.posicao_veiculo_geo, {{ var("buffer_segmento_metros") }}
            ) as indicador_intersecao
        from registros r
        left join shape_union s using (servico, data)
        left join
            {{ ref("shapes_geom_gtfs") }} sg using (
                {# `rj-smtr`.`gtfs`.`shapes_geom` sg using ( #}
                feed_start_date, shape_id
            )
    ),
    indicador as (
        select
            *,
            count(case when indicador_intersecao then 1 end) over (
                partition by id_veiculo
                order by
                    unix_seconds(timestamp(datetime_gps))
                    range
                    between {{ var("intervalo_max_desvio_segundos") }} preceding
                    and current row
            )
            >= 1 as indicador_trajeto_correto
        from intersec
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    logical_or(indicador_trajeto_correto) as indicador_trajeto_correto
from indicador
group by id_veiculo, servico, data, datetime_gps
