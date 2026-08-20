{{
    config(
        materialized="view",
    )
}}

with
    viagem_completa as (
        select
            id_viagem,
            data,
            id_empresa,
            id_veiculo,
            servico_informado as servico_gps,
            servico_realizado as servico,
            consorcio,
            "Ônibus" as modo,
            trip_id,
            cast(null as string) as route_id,
            shape_id,
            sentido,
            distancia_planejada,
            datetime_partida,
            datetime_chegada,
            versao_modelo as versao,
            datetime_ultima_atualizacao
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_inferida as (
        select
            id_viagem,
            data,
            id_empresa,
            id_veiculo,
            servico_gps,
            servico,
            consorcio,
            modo,
            trip_id,
            route_id,
            shape_id,
            sentido,
            distancia_planejada,
            datetime_partida,
            datetime_chegada,
            versao,
            datetime_ultima_atualizacao
        from {{ ref("viagem_inferida") }}
        where date(datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_inferida
