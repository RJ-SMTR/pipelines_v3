{{ config(materialized="view", alias="viagem_onibus") }}

with
    viagem_completa as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            "Ônibus" as modo,
            vista,
            tipo_dia,
            consorcio,
            servico_realizado as servico,
            shape_id,
            sentido,
            id_veiculo,
            distancia_planejada,
            tempo_viagem
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_valida as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            modo,
            vista,
            tipo_dia,
            consorcio,
            servico,
            shape_id,
            sentido,
            id_veiculo,
            distancia_planejada,
            datetime_diff(datetime_chegada, datetime_partida, minute) as tempo_viagem
        from {{ ref("viagem_valida") }}
        where date(datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_valida
