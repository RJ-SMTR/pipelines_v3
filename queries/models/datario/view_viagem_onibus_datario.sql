{{ config(materialized="view", alias="viagem_onibus") }}

with
    vista_shape as (
        select feed_start_date, shape_id, any_value(vista) as vista
        from {{ ref("ordem_servico_trips_shapes_gtfs") }}
        where feed_start_date >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
        group by feed_start_date, shape_id
    ),
    viagem_completa as (
        select
            data,
            consorcio,
            "Ônibus SPPO" as modo,
            tipo_dia,
            id_empresa,
            id_veiculo,
            id_viagem,
            servico_informado,
            servico_realizado,
            vista,
            shape_id,
            sentido,
            datetime_partida,
            datetime_chegada,
            inicio_periodo,
            fim_periodo,
            tempo_viagem,
            distancia_planejada,
            perc_conformidade_shape,
            perc_conformidade_registros,
            versao_modelo
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_valida as (
        select
            v.data,
            v.consorcio,
            v.modo,
            v.tipo_dia,
            regexp_extract(upper(v.id_veiculo), r"^[A-Z]([0-9]{3})") as id_empresa,
            v.id_veiculo,
            v.id_viagem,
            v.servico as servico_informado,
            v.servico as servico_realizado,
            vs.vista,
            v.shape_id,
            v.sentido,
            v.datetime_partida,
            v.datetime_chegada,
            cast(null as datetime) as inicio_periodo,
            cast(null as datetime) as fim_periodo,
            datetime_diff(
                v.datetime_chegada, v.datetime_partida, minute
            ) as tempo_viagem,
            v.distancia_planejada,
            cast(null as float64) as perc_conformidade_shape,
            cast(null as float64) as perc_conformidade_registros,
            v.versao as versao_modelo
        from {{ ref("viagem_valida") }} as v
        left join
            vista_shape as vs
            on vs.feed_start_date = v.feed_start_date
            and vs.shape_id = v.shape_id
        where date(v.datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_valida
