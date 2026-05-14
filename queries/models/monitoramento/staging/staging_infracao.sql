{{ config(materialized="view", alias="infracao") }}

select safe_cast(data as string) as data, * except (data)
from {{ ref("staging_infracao_v2") }}
where data >= date("{{ var('data_inicio_dbstu') }}")
union all by name
select *
from {{ ref("staging_infracao_v1") }}
where data < "{{ var('data_inicio_dbstu') }}"
