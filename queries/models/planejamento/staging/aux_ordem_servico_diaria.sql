{{ config(materialized="ephemeral") }}

select *
from {{ ref("aux_ordem_servico_diaria_v1") }}
where feed_start_date < date('{{ var("DATA_GTFS_V4_INICIO") }}')
union all by name
select *
from {{ ref("aux_ordem_servico_diaria_v2") }}
where feed_start_date >= date('{{ var("DATA_GTFS_V4_INICIO") }}')
