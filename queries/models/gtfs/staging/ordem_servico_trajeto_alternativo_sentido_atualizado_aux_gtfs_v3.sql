{{ config(materialized="ephemeral") }}
/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados
  e com atualização dos sentidos circulares
*/
with
    ordem_servico_trajeto_alternativo_sentido as (
        select
            * except (sentido, extensao),
            left(sentido, 1) as sentido,
            extensao as distancia_planejada
        from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
        where feed_start_date = date("{{ var('data_versao_gtfs') }}")
    ),
    ordem_servico_faixa_horaria_sentido as (
        select
            feed_start_date,
            servico,
            array_agg(distinct left(sentido, 1)) as sentido_array,
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        where feed_start_date = '{{ var("data_versao_gtfs") }}'
        group by 1, 2
    ),
    indicador_duplo_sentido as (
        select
            feed_start_date,
            feed_version,
            tipo_os,
            servico,
            evento,
            countif(sentido = "I" and distancia_planejada != 0) > 0
            and countif(sentido = "V" and distancia_planejada != 0)
            > 0 as indicador_duplo_sentido
        from ordem_servico_trajeto_alternativo_sentido
        group by 1, 2, 3, 4, 5
    )
select
    ot.* except (sentido),
    case
        when "C" in unnest(s.sentido_array) and not d.indicador_duplo_sentido
        then "C"
        else ot.sentido
    end as sentido
from ordem_servico_trajeto_alternativo_sentido as ot
left join ordem_servico_faixa_horaria_sentido as s using (feed_start_date, servico)
left join
    indicador_duplo_sentido as d using (
        feed_start_date, feed_version, tipo_os, servico, evento
    )
where ot.distancia_planejada != 0
