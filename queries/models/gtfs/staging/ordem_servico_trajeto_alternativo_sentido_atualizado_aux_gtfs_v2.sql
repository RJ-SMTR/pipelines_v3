{{ config(materialized="ephemeral") }}

/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/
with
    ordem_servico_trajeto_alternativo as (
        select *, (extensao_ida != 0 and extensao_volta != 0) as indicador_duplo_sentido
        from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
        where feed_start_date = "{{ var('data_versao_gtfs') }}"
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
    ordem_servico_trajeto_alternativo_sentido as (
        select *
        from
            ordem_servico_trajeto_alternativo unpivot (
                (distancia_planejada) for sentido
                in ((extensao_ida) as "I", (extensao_volta) as "V")
            )
    )
select
    * except (sentido, indicador_duplo_sentido),
    case
        when "C" in unnest(sentido_array) and not indicador_duplo_sentido
        then "C"
        else o.sentido
    end as sentido,
from ordem_servico_trajeto_alternativo_sentido as o
left join ordem_servico_faixa_horaria_sentido as s using (feed_start_date, servico)
where distancia_planejada != 0
