{{ config(materialized="ephemeral") }}

with
    routes as (
        select
            *,
            case
                when agency_id in ("22005", "22002", "22004", "22003")
                then "Ônibus"
                when agency_id = "20001"
                then "BRT"
            end as modo
        from {{ ref("routes_gtfs") }}
    ),
    trips as (
        select
            t.trip_id,
            r.modo,
            t.route_id,
            t.service_id,
            r.route_short_name as servico,
            t.direction_id,
            t.shape_id,
            t.feed_version,
            t.feed_start_date,
            regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
        from {{ ref("trips_gtfs") }} t
        join routes r using (feed_start_date, feed_version, route_id)
    ),
    trips_os as (
        select
            t.*,
            os.tipo_dia,
            os.tipo_os,
            os.distancia_total_planejada,
            os.feed_start_date is not null as indicador_possui_os,
            os.horario_inicio,
            os.horario_fim,
            os.extensao as extensao_os
        from trips t
        left join
            {{ ref("aux_ordem_servico_horario_tratado") }} os
            on t.feed_start_date = os.feed_start_date
            and t.feed_version = os.feed_version
            and t.servico = os.servico
            and (
                (os.sentido in ('I', 'C') and t.direction_id = '0')
                or (os.sentido = 'V' and t.direction_id = '1')
            )
    ),
    trajeto_alternativo_sentido as (
        select
            feed_start_date, feed_version, tipo_os, servico, evento, sentido, extensao
        from
            (
                select
                    feed_start_date,
                    feed_version,
                    tipo_os,
                    servico,
                    evento,
                    extensao_ida,
                    extensao_volta
                from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
                where feed_start_date < date('{{ var("DATA_GTFS_V4_INICIO") }}')
            ) unpivot (
                (extensao)
                for sentido in ((extensao_ida) as 'I', (extensao_volta) as 'V')
            )
        union all
        select
            feed_start_date,
            feed_version,
            tipo_os,
            servico,
            evento,
            left(sentido, 1) as sentido,
            extensao
        from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
        where feed_start_date >= date('{{ var("DATA_GTFS_V4_INICIO") }}')
    )
select
    t.* except (evento, extensao_os),
    tas.evento,
    ifnull(tas.extensao, t.extensao_os) as extensao
from trips_os t
left join
    trajeto_alternativo_sentido tas
    on t.feed_start_date = tas.feed_start_date
    and t.feed_version = tas.feed_version
    and t.tipo_os = tas.tipo_os
    and t.servico = tas.servico
    and t.evento = tas.evento
    and (
        (tas.sentido in ('I', 'C') and t.direction_id = '0')
        or (tas.sentido = 'V' and t.direction_id = '1')
    )
