{{ config(materialized="ephemeral") }}

-- depends_on: {{ ref('ordem_servico_trajeto_alternativo_gtfs') }}
-- depends_on: {{ ref('ordem_servico_trajeto_alternativo_sentido') }}
/*
Identificação de um trip de referência para cada serviço e sentido regular
Identificação de todas as trips de referência para os trajetos alternativos
*/
with
    shapes as (
        select *
        from {{ ref("shapes_geom_gtfs") }}
        where feed_start_date = '{{ var("data_versao_gtfs") }}'
    ),
    trips_all as (
        select
            *,
            case
                when indicador_trajeto_alternativo = true
                then
                    concat(
                        feed_version, trip_short_name, tipo_dia, direction_id, shape_id
                    )
                else concat(feed_version, trip_short_name, tipo_dia, direction_id)
            end as trip_partition
        from
            (
                select
                    service_id,
                    trip_id,
                    trip_headsign,
                    trip_short_name,
                    direction_id,
                    shape_id,
                    feed_version,
                    shape_distance,
                    start_pt,
                    end_pt,
                    case
                        when service_id like "%U_%"
                        then "Dia Útil"
                        when service_id like "%S_%"
                        then "Sabado"
                        when service_id like "%D_%"
                        then "Domingo"
                        else service_id
                    end as tipo_dia,
                    case
                        when
                            (
                                trip_headsign like "%[reveillon]%"
                                or trip_headsign like "%[desvio_obra]%"
                                or trip_headsign like "%[reversível]%"
                                or trip_headsign like "%[desvio_feira]%"
                                or trip_headsign like "%[desvio_lazer]%"
                                or trip_headsign like "%[desvio_túnel]%"
                                or trip_headsign like "%[desvio_januário]%"
                                or trip_headsign like "%[desvio_maracanã]%"
                                or trip_headsign like "%[excepcionalidade]%"
                                or trip_headsign like "%[excepcionalidade_1]%"
                                or trip_headsign like "%[excepcionalidade_2]%"
                                or service_id = "EXCEP"
                            )
                        then true
                        else false
                    end as indicador_trajeto_alternativo,
                from {{ ref("trips_gtfs") }}
                left join shapes using (feed_start_date, feed_version, shape_id)
                where
                    feed_start_date = '{{ var("data_versao_gtfs") }}'
                    and service_id not like "%_DESAT_%"
            )
    )
select * except (shape_distance),
from trips_all
qualify
    row_number() over (
        partition by trip_partition
        order by
            feed_version, trip_short_name, tipo_dia, direction_id, shape_distance desc
    )
    = 1
