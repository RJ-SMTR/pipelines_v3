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
    )
select
    td.* except (evento),
    osa.evento,
    case
        when td.direction_id = '0'
        then ifnull(osa.extensao_ida, os.extensao_ida)
        when td.direction_id = '1'
        then ifnull(osa.extensao_volta, os.extensao_volta)
    end as extensao,
    os.distancia_total_planejada,
    os.feed_start_date is not null as indicador_possui_os,
    os.horario_inicio,
    os.horario_fim,
from trips_dia td
left join
    {{ ref("ordem_servico_trajeto_alternativo_gtfs") }} osa using (
        feed_start_date, feed_version, tipo_os, servico, evento
    )
{# `rj-smtr.gtfs.ordem_servico_trajeto_alternativo` osa using (
        feed_start_date, feed_version, tipo_os, servico, evento
    ) #}
left join
    {{ ref("aux_ordem_servico_horario_tratado") }} os using (
        feed_start_date, feed_version, tipo_os, tipo_dia, servico
    )
