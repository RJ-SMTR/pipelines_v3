{{
    config(
        materialized="view",
    )
}}

with
    vista_shape as (
        select feed_start_date, shape_id, any_value(vista) as vista
        from {{ ref("ordem_servico_trips_shapes_gtfs") }}
        where feed_start_date >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
        group by feed_start_date, shape_id
    ),
    calendario as (select data, tipo_dia, feed_start_date from {{ ref("calendario") }}),
    viagem_completa as (
        select
            id_viagem,
            data,
            id_empresa,
            id_veiculo,
            servico_informado,
            servico_realizado,
            consorcio,
            "Ônibus SPPO" as modo,
            tipo_dia,
            trip_id,
            cast(null as string) as route_id,
            shape_id,
            sentido,
            vista,
            distancia_planejada,
            datetime_partida,
            datetime_chegada,
            inicio_periodo,
            fim_periodo,
            tempo_viagem,
            versao_modelo as versao,
            datetime_ultima_atualizacao
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_inferida as (
        select
            v.id_viagem,
            v.data,
            v.id_empresa,
            v.id_veiculo,
            v.servico_gps as servico_informado,
            v.servico as servico_realizado,
            v.consorcio,
            v.modo,
            c.tipo_dia,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.sentido,
            vs.vista,
            v.distancia_planejada,
            v.datetime_partida,
            v.datetime_chegada,
            cast(null as datetime) as inicio_periodo,
            cast(null as datetime) as fim_periodo,
            datetime_diff(
                v.datetime_chegada, v.datetime_partida, minute
            ) as tempo_viagem,
            v.versao,
            v.datetime_ultima_atualizacao
        from {{ ref("viagem_inferida") }} as v
        left join calendario as c using (data)
        left join
            vista_shape as vs
            on vs.feed_start_date = c.feed_start_date
            and vs.shape_id = v.shape_id
        where date(v.datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_inferida
