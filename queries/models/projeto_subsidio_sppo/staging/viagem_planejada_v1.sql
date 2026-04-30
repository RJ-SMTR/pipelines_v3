{{
    config(
        materialized="ephemeral",
    )
}}
-- 1. Define datas do período planejado
with
    data_efetiva as (
        select
            data,
            tipo_dia,
            data_versao_shapes,
            data_versao_trips,
            data_versao_frequencies
        from {{ ref("subsidio_data_versao_efetiva") }}
        where
            data between date_sub("{{ var('run_date') }}", interval 1 day) and date(
                "{{ var('run_date') }}"
            )
            and data < date('{{ var("DATA_SUBSIDIO_V6_INICIO") }}')
    ),
    -- 2. Puxa dados de distancia quadro no quadro horário
    quadro as (
        select
            e.data,
            e.tipo_dia,
            p.* except (tipo_dia, data_versao, horario_inicio, horario_fim),
            if(
                horario_inicio is not null
                and array_length(split(horario_inicio, ":")) = 3,
                datetime_add(
                    datetime(
                        e.data,
                        parse_time(
                            "%T",
                            concat(
                                safe_cast(
                                    mod(
                                        safe_cast(
                                            split(horario_inicio, ":")[
                                                offset(0)
                                            ] as int64
                                        ),
                                        24
                                    ) as int64
                                ),
                                ":",
                                safe_cast(
                                    split(horario_inicio, ":")[offset(1)] as int64
                                ),
                                ":",
                                safe_cast(
                                    split(horario_inicio, ":")[offset(2)] as int64
                                )
                            )
                        )
                    ),
                    interval div(
                        safe_cast(split(horario_inicio, ":")[offset(0)] as int64), 24
                    ) day
                ),
                null
            ) as inicio_periodo,
            if(
                horario_fim is not null and array_length(split(horario_fim, ":")) = 3,
                datetime_add(
                    datetime(
                        e.data,
                        parse_time(
                            "%T",
                            concat(
                                safe_cast(
                                    mod(
                                        safe_cast(
                                            split(horario_fim, ":")[offset(0)] as int64
                                        ),
                                        24
                                    ) as int64
                                ),
                                ":",
                                safe_cast(split(horario_fim, ":")[offset(1)] as int64),
                                ":",
                                safe_cast(split(horario_fim, ":")[offset(2)] as int64)
                            )
                        )
                    ),
                    interval div(
                        safe_cast(split(horario_fim, ":")[offset(0)] as int64), 24
                    ) day
                ),
                null
            ) as fim_periodo
        from data_efetiva e
        inner join
            (
                select *
                from {{ ref("subsidio_quadro_horario") }}
                where data_versao in (select data_versao_frequencies from data_efetiva)
            ) p
            on e.data_versao_frequencies = p.data_versao
            and e.tipo_dia = p.tipo_dia
    ),
    -- 3. Trata informação de trips: adiciona ao sentido da trip o sentido
    -- planejado (os shapes/trips circulares são separados em
    -- ida/volta no sigmob)
    trips as (
        select e.data, t.*
        from
            (
                select *
                from {{ ref("subsidio_trips_desaninhada") }}
                where data_versao in (select data_versao_trips from data_efetiva)
            ) t
        inner join data_efetiva e on t.data_versao = e.data_versao_trips
    ),
    quadro_trips as (
        select *
        from
            (
                select distinct
                    * except (trip_id), trip_id as trip_id_planejado, trip_id
                from quadro
                where sentido = "I" or sentido = "V"
            )
        union all
        (
            select
                * except (trip_id),
                trip_id as trip_id_planejado,
                concat(trip_id, "_0") as trip_id,
            from quadro
            where sentido = "C"
        )
        union all
        (
            select
                * except (trip_id),
                trip_id as trip_id_planejado,
                concat(trip_id, "_1") as trip_id,
            from quadro
            where sentido = "C"
        )
    ),
    quadro_tratada as (
        select
            q.*,
            t.shape_id as shape_id_planejado,
            case
                when sentido = "C"
                then shape_id || "_" || split(q.trip_id, "_")[offset(1)]
                else shape_id
            end as shape_id,  -- TODO: adicionar no sigmob
        from quadro_trips q
        left join trips t on t.data = q.data and t.trip_id = q.trip_id_planejado
    ),
    -- 4. Trata informações de shapes: junta trips e shapes para resgatar o sentido
    -- planejado (os shapes/trips circulares são separados em
    -- ida/volta no sigmob)
    shapes as (
        select e.data, data_versao as data_shape, shape_id, shape, start_pt, end_pt
        from data_efetiva e
        inner join
            (
                select *
                from {{ ref("subsidio_shapes_geom") }}
                where data_versao in (select data_versao_shapes from data_efetiva)
            ) s
            on s.data_versao = e.data_versao_shapes
    )
-- 5. Junta shapes e trips aos servicos planejados no quadro horário
select
    p.*,
    s.data_shape,
    s.shape,
    case
        when p.sentido = "C" and split(p.shape_id, "_")[offset(1)] = "0"
        then "I"
        when p.sentido = "C" and split(p.shape_id, "_")[offset(1)] = "1"
        then "V"
        when p.sentido = "I" or p.sentido = "V"
        then p.sentido
    end as sentido_shape,
    safe_cast(null as int64) as partidas_total_planejada,  -- Adaptação para formato da SUBSIDIO_V6
    s.start_pt,
    s.end_pt,
    safe_cast(null as int64) as id_tipo_trajeto,  -- Adaptação para formato da SUBSIDIO_V6
    safe_cast(null as date) as feed_start_date,  -- Adaptação para formato da SUBSIDIO_V6
    safe_cast(null as string) as feed_version,  -- Adaptação para formato da SUBSIDIO_V6
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,  -- Adaptação para formato da SUBSIDIO_V7
    datetime(concat(p.data, "T00:00:00")) as faixa_horaria_inicio,  -- Adaptação para formato da SUBSIDIO_V9
    datetime(concat(p.data, "T23:59:59")) as faixa_horaria_fim,  -- Adaptação para formato da SUBSIDIO_V9
from quadro_tratada p
inner join shapes s on p.shape_id = s.shape_id and p.data = s.data
