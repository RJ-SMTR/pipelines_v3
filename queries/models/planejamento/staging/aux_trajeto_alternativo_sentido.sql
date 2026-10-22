{{ config(materialized="ephemeral") }}

select *
from {{ ref("aux_trajeto_alternativo_sentido_v1") }}
where feed_start_date < date('{{ var("DATA_GTFS_V4_INICIO") }}')
union all by name
select *
from {{ ref("aux_trajeto_alternativo_sentido_v2") }}
where feed_start_date >= date('{{ var("DATA_GTFS_V4_INICIO") }}')
