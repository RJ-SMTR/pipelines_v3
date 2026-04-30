{{
    config(
        materialized="ephemeral",
    )
}}

-- 1. Identifica registros pertencentes a viagens
with
    registros_viagem as (
        select s.* except (versao_modelo), datetime_partida, datetime_chegada, id_viagem
        from {{ ref("aux_monitoramento_registros_status_trajeto") }} s
        left join
            (
                select
                    id_veiculo, trip_id, id_viagem, datetime_partida, datetime_chegada
                from {{ ref("viagem_inferida") }}
            ) v
            on s.id_veiculo = v.id_veiculo
            and s.trip_id = v.trip_id
            and s.timestamp_gps between v.datetime_partida and v.datetime_chegada
    )
-- 2. Filtra apenas registros de viagens identificadas
select *
from registros_viagem
where id_viagem is not null
