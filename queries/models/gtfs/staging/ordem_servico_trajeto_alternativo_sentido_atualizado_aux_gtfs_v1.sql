{{ config(materialized="ephemeral") }}

/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/
with
    ordem_servico_trajeto_alternativo as (
        select *
        from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
        where feed_start_date = "{{ var('data_versao_gtfs') }}"
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
    * except (sentido),
    case when "C" in unnest(sentido_array) then "C" else o.sentido end as sentido,
from ordem_servico_trajeto_alternativo_sentido as o
left join
    (
        select feed_start_date, servico, array_agg(distinct sentido) as sentido_array,
        from {{ ref("ordem_servico_sentido_atualizado_aux_gtfs") }}
        group by 1, 2
    ) as s using (feed_start_date, servico)
where distancia_planejada != 0
