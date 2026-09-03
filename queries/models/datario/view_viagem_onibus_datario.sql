{{ config(materialized="view", alias="viagem_onibus") }}

with
    viagem_completa as (
        select
            data,
            consorcio,
            "Ônibus" as modo,
            tipo_dia,
            id_empresa,
            id_veiculo,
            id_viagem,
            servico_realizado as servico,
            vista,
            shape_id,
            sentido,
            datetime_partida,
            datetime_chegada,
            tempo_viagem,
            distancia_planejada
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_valida as (
        select
            data,
            consorcio,
            modo,
            tipo_dia,
            regexp_extract(upper(id_veiculo), r"^[A-Z]([0-9]{3})") as id_empresa,
            id_veiculo,
            id_viagem,
            servico,
            vista,
            shape_id,
            sentido,
            datetime_partida,
            datetime_chegada,
            datetime_diff(datetime_chegada, datetime_partida, minute) as tempo_viagem,
            distancia_planejada
        from {{ ref("viagem_valida") }}
        where date(datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_valida
