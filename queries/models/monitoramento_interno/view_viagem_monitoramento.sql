{{
    config(
        materialized="view",
    )
}}

with
    viagem_completa as (
        select
            data,
            id_viagem,
            id_empresa,
            datetime_partida,
            datetime_chegada,
            "Ônibus" as modo,
            "SPPO" as sistema,
            vista,
            tipo_dia,
            consorcio,
            servico_informado as servico_gps,
            servico_realizado as servico,
            cast(null as string) as route_id,
            trip_id,
            shape_id,
            sentido,
            id_veiculo,
            distancia_planejada,
            datetime_ultima_atualizacao,
            versao_modelo as versao
        from {{ ref("viagem_completa") }}
        where date(datetime_partida) < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    ),
    viagem_valida as (
        select
            data,
            id_viagem,
            regexp_extract(upper(id_veiculo), r"^[A-Z]([0-9]{3})") as id_empresa,
            datetime_partida,
            datetime_chegada,
            modo,
            sistema,
            vista,
            tipo_dia,
            consorcio,
            servico as servico_gps,
            servico,
            route_id,
            trip_id,
            shape_id,
            sentido,
            id_veiculo,
            distancia_planejada,
            datetime_ultima_atualizacao,
            versao
        from {{ ref("viagem_valida") }}
        where date(datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_valida
