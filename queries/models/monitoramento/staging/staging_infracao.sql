{{ config(materialized="view", alias="infracao") }}

with
    v1_normalizada as (
        select safe_cast(data as date) as data, * except (data)
        from {{ ref("staging_infracao_v1") }}
    )

select *
from {{ ref("staging_infracao_v2") }}
where data >= date("{{ var('data_inicio_dbstu') }}")
union all by name
select *
from v1_normalizada
where data < date("{{ var('data_inicio_dbstu') }}")
