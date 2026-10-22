{{ config(materialized="ephemeral") }}

with
    shapes as (
        select *
        from {{ ref("shapes_geom_gtfs") }}
        where feed_start_date = '{{ var("data_versao_gtfs") }}'
    ),
    ordem_servico_tratada as (
        select *
        from
            (
                (
                    select
                        o.feed_version,
                        o.feed_start_date,
                        o.feed_end_date,
                        o.tipo_os,
                        o.tipo_dia,
                        servico,
                        vista,
                        consorcio,
                        sentido,
                        distancia_planejada,
                        distancia_total_planejada,
                        inicio_periodo,
                        fim_periodo,
                        trip_id,
                        shape_id,
                        indicador_trajeto_alternativo
                    from {{ ref("ordem_servico_sentido_atualizado_aux_gtfs") }} as o
                    left join
                        {{ ref("trips_filtrada_aux_gtfs") }} as t
                        on t.feed_version = o.feed_version
                        and o.servico = t.trip_short_name
                        and (
                            (
                                o.tipo_dia = t.tipo_dia
                                and o.tipo_os not in ("CNU", "Enem", "Eleição")
                            )
                            or (
                                o.tipo_dia = "Ponto Facultativo"
                                and t.tipo_dia = "Dia Útil"
                                and o.tipo_os != "CNU"
                            )
                            or (
                                o.feed_start_date = "2024-08-16"
                                and o.tipo_os = "CNU"
                                and o.tipo_dia = "Domingo"
                                and t.tipo_dia = "Sabado"
                            )
                            or (
                                o.feed_start_date in ("2024-09-29", "2024-11-06")
                                and o.tipo_os = "Enem"
                                and o.tipo_dia = "Domingo"
                                and t.tipo_dia = "Sabado"
                            )
                            or (
                                o.feed_start_date in ("2024-09-29")
                                and o.tipo_os = "Eleição"
                                and o.tipo_dia in ("Domingo", "Sabado", "Dia Útil")
                                and t.tipo_dia in ("Dia Útil")
                            )
                        )
                        and (
                            (o.sentido in ("I", "C") and t.direction_id = "0")
                            or (o.sentido = "V" and t.direction_id = "1")
                        )
                    where indicador_trajeto_alternativo is false
                )
                union all
                (
                    select
                        o.feed_version,
                        o.feed_start_date,
                        o.feed_end_date,
                        o.tipo_os,
                        o.tipo_dia,
                        servico,
                        o.vista || " " || ot.evento as vista,
                        o.consorcio,
                        sentido,
                        ot.distancia_planejada,
                        distancia_total_planejada,
                        coalesce(ot.inicio_periodo, o.inicio_periodo) as inicio_periodo,
                        coalesce(ot.fim_periodo, o.fim_periodo) as fim_periodo,
                        trip_id,
                        shape_id,
                        indicador_trajeto_alternativo
                    from
                        {{
                            ref(
                                "ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs"
                            )
                        }}
                        as ot
                    left join
                        {{ ref("ordem_servico_sentido_atualizado_aux_gtfs") }} as o
                        using (feed_version, tipo_os, servico, sentido)
                    left join
                        {{ ref("trips_filtrada_aux_gtfs") }} as t
                        on t.feed_version = o.feed_version
                        and o.servico = t.trip_short_name
                        and (
                            o.tipo_dia = t.tipo_dia
                            or (
                                o.tipo_dia = "Ponto Facultativo"
                                and t.tipo_dia = "Dia Útil"
                            )
                            or (t.tipo_dia = "EXCEP")
                        )
                        and (
                            (o.sentido in ("I", "C") and t.direction_id = "0")
                            or (o.sentido = "V" and t.direction_id = "1")
                        )
                        and t.trip_headsign like concat("%", ot.evento, "%")
                    where indicador_trajeto_alternativo is true and trip_id is not null
                )
            )
    ),
    ordem_servico_trips as (
        select
            * except (shape_id, indicador_trajeto_alternativo),
            shape_id as shape_id_planejado,
            case
                when sentido = "C"
                then shape_id || "_" || split(trip_id, "_")[offset(1)]
                else shape_id
            end as shape_id,
            case
                when indicador_trajeto_alternativo is false
                then 0
                when indicador_trajeto_alternativo is true
                then 1
            end as id_tipo_trajeto
        from
            (
                (
                    select distinct
                        * except (trip_id), trip_id as trip_id_planejado, trip_id
                    from ordem_servico_tratada
                    where sentido = "I" or sentido = "V"
                )
                union all
                (
                    select
                        * except (trip_id),
                        trip_id as trip_id_planejado,
                        concat(trip_id, "_0") as trip_id,
                    from ordem_servico_tratada
                    where sentido = "C"
                )
                union all
                (
                    select
                        * except (trip_id),
                        trip_id as trip_id_planejado,
                        concat(trip_id, "_1") as trip_id,
                    from ordem_servico_tratada
                    where sentido = "C"
                )
            )
    )
select *
from
    (
        select
            feed_version,
            feed_start_date,
            o.feed_end_date,
            tipo_os,
            tipo_dia,
            servico,
            o.vista,
            o.consorcio,
            sentido,
            case
                when feed_start_date >= '2024-08-16' then fh.partidas else null
            end as partidas_total_planejada,
            distancia_planejada,
            case
                when feed_start_date >= '2024-08-16'
                then fh.quilometragem
                else distancia_total_planejada
            end as distancia_total_planejada,
            inicio_periodo,
            fim_periodo,
            case
                when feed_start_date >= '2024-08-16'
                then fh.faixa_horaria_inicio
                else "00:00:00"
            end as faixa_horaria_inicio,
            case
                when feed_start_date >= '2024-08-16'
                then fh.faixa_horaria_fim
                else "23:59:59"
            end as faixa_horaria_fim,
            trip_id_planejado,
            trip_id,
            shape_id,
            shape_id_planejado,
            shape,
            case
                when sentido = "C" and split(shape_id, "_")[offset(1)] = "0"
                then "I"
                when sentido = "C" and split(shape_id, "_")[offset(1)] = "1"
                then "V"
                when sentido = "I" or sentido = "V"
                then sentido
            end as sentido_shape,
            s.start_pt,
            s.end_pt,
            id_tipo_trajeto,
        from ordem_servico_trips as o
        left join shapes as s using (feed_version, feed_start_date, shape_id)
        left join
            {{ ref("ordem_servico_faixa_horaria") }} as fh using (
                feed_version, feed_start_date, tipo_os, tipo_dia, servico
            )
        where
            feed_start_date = '{{ var("data_versao_gtfs") }}'
            and (
                (
                    feed_start_date >= '2024-08-16'
                    and (
                        fh.quilometragem != 0
                        and (fh.partidas != 0 or fh.partidas is null)
                    )
                )
                or feed_start_date < '2024-08-16'
            )
            and feed_start_date < '{{ var("DATA_GTFS_V4_INICIO") }}'
    )
