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
        {# from `rj-smtr.gtfs.routes` #}
        from {{ ref("routes_gtfs") }}
    ),
    trips_dia as (
        select
            c.data,
            t.trip_id,
            r.modo,
            t.route_id,
            t.service_id,
            r.route_short_name as servico,
            t.direction_id,
            t.shape_id,
            c.tipo_dia,
            c.subtipo_dia,
            c.tipo_os,
            t.feed_version,
            t.feed_start_date,
            regexp_extract(t.trip_headsign, r'\[.*?\]') as evento
        {# from `rj-smtr.planejamento.calendario` c #}
        from {{ ref("calendario") }} c
        {# join `rj-smtr.gtfs.trips` t using (feed_start_date, feed_version) #}
        join {{ ref("trips_gtfs") }} t using (feed_start_date, feed_version)
        join routes r using (feed_start_date, feed_version, route_id)
        where t.service_id in unnest(c.service_ids)
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
    td.* except (evento),
    tas.evento,
    ifnull(tas.extensao, os.extensao) as extensao,
    os.distancia_total_planejada,
    os.feed_start_date is not null as indicador_possui_os,
    os.horario_inicio,
    os.horario_fim,
from trips_dia td
left join
    trajeto_alternativo_sentido tas
    on td.feed_start_date = tas.feed_start_date
    and td.feed_version = tas.feed_version
    and td.tipo_os = tas.tipo_os
    and td.servico = tas.servico
    and td.evento = tas.evento
    and (
        (tas.sentido in ('I', 'C') and td.direction_id = '0')
        or (tas.sentido = 'V' and td.direction_id = '1')
    )
left join
    {{ ref("aux_ordem_servico_horario_tratado") }} os
    on td.feed_start_date = os.feed_start_date
    and td.feed_version = os.feed_version
    and td.tipo_os = os.tipo_os
    and td.tipo_dia = os.tipo_dia
    and td.servico = os.servico
    and (
        (os.sentido in ('I', 'C') and td.direction_id = '0')
        or (os.sentido = 'V' and td.direction_id = '1')
    )
