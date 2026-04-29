{{ config(materialized="ephemeral") }}

/*
  ordem_servico_trajeto_alternativo_gtfs com sentidos despivotados e com atualização dos sentidos circulares
*/
select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v1") }}
where
    feed_start_date < date("{{ var('DATA_GTFS_V4_INICIO') }}")
--fmt:off
full outer union all by name
--fmt:on
select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v2") }}
where
    feed_start_date between date("{{ var('DATA_GTFS_V4_INICIO') }}") and date_sub(
        date("{{ var('DATA_GTFS_V5_INICIO') }}"), interval 1 day
    )
--fmt:off
full outer union all by name
--fmt:on
select *
from {{ ref("ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v3") }}
where feed_start_date >= date("{{ var('DATA_GTFS_V5_INICIO') }}")
