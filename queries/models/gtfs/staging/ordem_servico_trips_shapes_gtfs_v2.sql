{{ config(materialized="ephemeral") }}

with
    shapes as (
        select *
        from {{ ref("shapes_geom_gtfs") }}
        where feed_start_date = '{{ var("data_versao_gtfs") }}'
    ),
    ordem_servico_faixa_horaria_sentido as (
        select * except (sentido), left(sentido, 1) as sentido
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        where
            feed_start_date = '{{ var("data_versao_gtfs") }}'
            and (quilometragem != 0 and (partidas != 0 or partidas is null))
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
                        extensao as distancia_planejada,
                        quilometragem as distancia_total_planejada,
                        cast(null as string) as inicio_periodo,
                        cast(null as string) as fim_periodo,
                        partidas as partidas_total_planejada,
                        faixa_horaria_inicio as faixa_horaria_inicio,
                        faixa_horaria_fim as faixa_horaria_fim,
                        trip_id,
                        shape_id,
                        indicador_trajeto_alternativo
                    from ordem_servico_faixa_horaria_sentido as o
                    left join
                        {{ ref("trips_filtrada_aux_gtfs") }} as t
                        on t.feed_version = o.feed_version
                        and o.servico = t.trip_short_name
                        and (
                            (o.sentido in ("I", "C") and t.direction_id = "0")
                            or (o.sentido = "V" and t.direction_id = "1")
                        )
                        and (
                            (
                                o.tipo_dia = t.tipo_dia
                                and o.tipo_os not in ("ENEM", "V+ENEM")
                            )
                            or (
                                o.feed_start_date = "2026-01-26"
                                and o.tipo_dia = "Ponto Facultativo"
                                and t.tipo_dia = "Sabado"
                                and o.servico in ("LECD126")
                            )
                            or (
                                o.tipo_dia = "Ponto Facultativo"
                                and t.tipo_dia = "Dia Útil"
                            )
                            or (
                                o.feed_start_date = "2025-11-08"
                                and o.tipo_os in ("ENEM", "V+ENEM")
                                and o.tipo_dia = "Domingo"
                                and t.tipo_dia = "Dia Útil"
                                and o.servico not in ("LECD126", "SE867")
                            )
                            or (
                                o.feed_start_date = "2025-11-08"
                                and o.tipo_os in ("ENEM", "V+ENEM")
                                and o.tipo_dia = t.tipo_dia
                                and o.servico in ("LECD126", "SE867")
                            )
                            or (
                                o.feed_start_date = "2025-12-21"
                                and o.tipo_os = "Verão"
                                and t.tipo_dia = "Dia Útil"
                                and (
                                    (
                                        o.servico = '616'
                                        and o.tipo_dia in ('Sabado', 'Domingo')
                                    )
                                    or (o.servico = '913' and o.tipo_dia = 'Domingo')
                                    or (o.servico = '485' and o.tipo_dia = 'Sabado')
                                )
                            )
                            or (
                                o.feed_start_date = "2025-12-27"
                                and o.tipo_os = "Reveillon_01-01"
                                and t.tipo_dia = "Sabado"
                                and (
                                    o.servico in (
                                        "SE457",
                                        "SE474",
                                        "SE553",
                                        "SE397",
                                        "SE388",
                                        "SE100",
                                        "SE298",
                                        "SE355",
                                        "SE363",
                                        "SE265",
                                        "SE169",
                                        "SE483",
                                        "SE324",
                                        "SE383",
                                        "SE393",
                                        "SE554",
                                        "SE238",
                                        "SE399",
                                        "SE550",
                                        "SEB232",
                                        "SE312",
                                        "SE328",
                                        "SE368"
                                    )
                                    and o.tipo_dia = 'Domingo'
                                )
                            )
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
                        quilometragem as distancia_total_planejada,
                        cast(null as string) as inicio_periodo,
                        cast(null as string) as fim_periodo,
                        o.partidas as partidas_total_planejada,
                        o.faixa_horaria_inicio as faixa_horaria_inicio,
                        o.faixa_horaria_fim as faixa_horaria_fim,
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
                        ordem_servico_faixa_horaria_sentido as o using (
                            feed_version, tipo_os, servico, sentido
                        )
                    left join
                        {{ ref("trips_filtrada_aux_gtfs") }} as t
                        on t.feed_version = o.feed_version
                        and o.servico = t.trip_short_name
                        and (
                            (o.sentido in ("I", "C") and t.direction_id = "0")
                            or (o.sentido = "V" and t.direction_id = "1")
                        )
                        and (
                            (
                                o.tipo_dia = t.tipo_dia
                                and o.tipo_os not in ("ENEM", "V+ENEM")
                            )
                            or (
                                o.feed_start_date = "2026-01-26"
                                and o.tipo_dia = "Ponto Facultativo"
                                and t.tipo_dia = "Sabado"
                                and o.servico in ("LECD126")
                            )
                            or (
                                o.tipo_dia = "Ponto Facultativo"
                                and t.tipo_dia = "Dia Útil"
                            )
                            or (
                                o.feed_start_date = "2025-11-08"
                                and o.tipo_os in ("ENEM", "V+ENEM")
                                and o.tipo_dia = "Domingo"
                                and t.tipo_dia = "Dia Útil"
                                and o.servico not in ("LECD126", "SE867")
                            )
                            or (
                                o.feed_start_date = "2025-11-08"
                                and o.tipo_os in ("ENEM", "V+ENEM")
                                and o.tipo_dia = t.tipo_dia
                                and o.servico in ("LECD126", "SE867")
                            )
                            or (
                                o.feed_start_date = "2025-12-21"
                                and o.tipo_os = "Verão"
                                and t.tipo_dia = "Dia Útil"
                                and (
                                    (
                                        o.servico = '616'
                                        and o.tipo_dia in ('Sabado', 'Domingo')
                                    )
                                    or (o.servico = '913' and o.tipo_dia = 'Domingo')
                                    or (o.servico = '485' and o.tipo_dia = 'Sabado')
                                )
                            )
                            or (
                                o.feed_start_date = "2025-12-27"
                                and o.tipo_os = "Reveillon_01-01"
                                and t.tipo_dia = "Sabado"
                                and (
                                    o.servico in (
                                        "SE457",
                                        "SE474",
                                        "SE553",
                                        "SE397",
                                        "SE388",
                                        "SE100",
                                        "SE298",
                                        "SE355",
                                        "SE363",
                                        "SE265",
                                        "SE169",
                                        "SE483",
                                        "SE324",
                                        "SE383",
                                        "SE393",
                                        "SE554",
                                        "SE238",
                                        "SE399",
                                        "SE550",
                                        "SEB232",
                                        "SE312",
                                        "SE328",
                                        "SE368"
                                    )
                                    and o.tipo_dia = 'Domingo'
                                )
                            )
                            or (t.tipo_dia = "EXCEP")
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
                        concat(trip_id, "_0") as trip_id
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
            o.sentido,
            partidas_total_planejada,
            distancia_planejada,
            distancia_total_planejada,
            inicio_periodo,
            fim_periodo,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            trip_id_planejado,
            trip_id,
            shape_id,
            shape_id_planejado,
            shape,
            case
                when o.sentido = "C" and split(shape_id, "_")[offset(1)] = "0"
                then "I"
                when o.sentido = "C" and split(shape_id, "_")[offset(1)] = "1"
                then "V"
                when o.sentido = "I" or o.sentido = "V"
                then o.sentido
            end as sentido_shape,
            s.start_pt,
            s.end_pt,
            id_tipo_trajeto
        from ordem_servico_trips as o
        left join shapes as s using (feed_version, feed_start_date, shape_id)
        where
            feed_start_date = '{{ var("data_versao_gtfs") }}'
            and feed_start_date >= '{{ var("DATA_GTFS_V4_INICIO") }}'
    )
