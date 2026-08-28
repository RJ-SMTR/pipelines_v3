{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    aux_status as (
        select
            * except (servico_viagem),
            servico_viagem as servico,
            case
                when status_viagem = 'end'
                then
                    last_value(
                        case when status_viagem = 'start' then datetime_gps end
                    ) over (
                        partition by id_veiculo, shape_id
                        order by datetime_gps
                        rows between unbounded preceding and 1 preceding
                    )
            end as datetime_partida
        from {{ ref("aux_monitoramento_registros_status_trajeto") }}
        where indicador_intersecao_segmento = true
    ),
    viagens as (
        select
            data,
            concat(
                id_veiculo,
                "-",
                servico,
                "-",
                sentido,
                "-",
                shape_id,
                "-",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem,
            cast(null as string) as id_viagem_planejada,
            datetime_partida,
            datetime_gps as datetime_chegada,
            modo,
            consorcio,
            sistema,
            servico_gps,
            servico,
            route_id,
            trip_id,
            shape_id,
            sentido,
            id_veiculo,
            fonte_gps,
            distancia_planejada,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ var("version") }}' as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from aux_status
        where
            status_viagem = 'end'
            and datetime_partida is not null
            and datetime_partida < datetime_gps
        qualify
            row_number() over (
                partition by id_veiculo, shape_id, datetime_partida
                order by datetime_gps
            )
            = 1
    )
select *
from viagens v
