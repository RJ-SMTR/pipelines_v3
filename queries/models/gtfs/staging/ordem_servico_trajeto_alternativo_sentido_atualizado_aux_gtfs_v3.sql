{{ config(materialized="ephemeral") }}

/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/
select
    * except (sentido, extensao),
    left(sentido, 1) as sentido,
    extensao as distancia_planejada
from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
where feed_start_date = date("{{ var('data_versao_gtfs') }}")
