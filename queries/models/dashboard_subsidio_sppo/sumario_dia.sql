with
    planejado as (
        select
            consorcio,
            data,
            tipo_dia,
            trip_id_planejado as trip_id,
            servico,
            sentido,
            case
                when sentido = "C"
                then max(distancia_planejada)
                else sum(distancia_planejada)
            end as distancia_planejada,
            case
                when sentido = "C"
                then max(distancia_total_planejada)
                else sum(distancia_total_planejada)
            end as distancia_total_planejada,
            null as viagens_planejadas
        from {{ ref("viagem_planejada") }}
        -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
        where
            data >= "2022-06-01"
            and data < date("{{ var('DATA_SUBSIDIO_V2_INICIO') }}")
            and distancia_total_planejada > 0
        group by 1, 2, 3, 4, 5, 6
    ),
    viagem as (
        select data, trip_id, count(id_viagem) as viagens_realizadas
        from {{ ref("viagem_completa") }}
        -- `rj-smtr`.`projeto_subsidio_sppo`.`viagem_completa`
        where
            data >= "2022-06-01" and data < date("{{ var('DATA_SUBSIDIO_V2_INICIO') }}")
        group by 1, 2
    ),
    sumario as (
        select
            consorcio,
            data,
            tipo_dia,
            servico,
            distancia_planejada,
            null as viagens_planejadas,
            ifnull(sum(v.viagens_realizadas), 0) as viagens_subsidio,
            distancia_total_planejada,
            (
                ifnull(sum(v.viagens_realizadas), 0) * distancia_planejada
            ) as distancia_total_subsidio
        from planejado as p
        left join viagem as v using (trip_id, data)
        group by 1, 2, 3, 4, 5, 8
    ),
    sumario_agg as (
        select
            consorcio,
            data,
            tipo_dia,
            servico,
            null as viagens_planejadas,
            ifnull(sum(viagens_subsidio), 0) as viagens_subsidio,
            distancia_total_planejada,
            round(sum(distancia_total_subsidio), 3) as distancia_total_subsidio
        from sumario
        group by 1, 2, 3, 4, 5, 7
    ),
    valor as (
        select
            s.*,
            v.valor_subsidio_por_km,
            round(
                distancia_total_subsidio * v.valor_subsidio_por_km, 2
            ) as valor_total_aferido,
            if
            (
                distancia_total_planejada = 0,
                null,
                round(100 * distancia_total_subsidio / distancia_total_planejada, 2)
            ) as perc_distancia_total_subsidio
        from sumario_agg s
        left join
            (
                select *
                from {{ ref("subsidio_data_versao_efetiva") }}
                -- `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
                where
                    data >= "2022-06-01"
                    and data < date("{{ var('DATA_SUBSIDIO_V2_INICIO') }}")
            ) as v
            on v.data = s.data
    )
select
    *,
    case
        when
            (perc_distancia_total_subsidio < 80)
            or (perc_distancia_total_subsidio is null)
        then 0
        else valor_total_aferido
    end as valor_total_subsidio
from valor
