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
            "SPPO" as sistema,
            "Ônibus" as modo,
            cast(null as string) as route_id,
            trip_id,
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
    viagem_valida as (
        select
            id_viagem,
            data,
            regexp_extract(upper(id_veiculo), r"^[A-Z]([0-9]{3})") as id_empresa,
            id_veiculo,
            cast(null as string) as servico_gps,
            servico,
            consorcio,
            sistema,
            modo,
            route_id,
            trip_id,
            shape_id,
            sentido,
            distancia_planejada,
            datetime_partida,
            datetime_chegada,
            versao,
            datetime_ultima_atualizacao
        from {{ ref("viagem_valida") }}
        where date(datetime_partida) >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
    )
select *
from viagem_completa
union all by name
select *
from viagem_valida
