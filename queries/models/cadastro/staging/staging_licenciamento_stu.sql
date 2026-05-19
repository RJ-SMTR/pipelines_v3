{{ config(materialized="view", alias="licenciamento_stu") }}

select * except(data), safe_cast(data as string) as data
from {{ ref("staging_licenciamento_stu_v2") }}
where data >= date("{{ var('data_inicio_dbstu') }}")
union all by name
select *
from {{ ref("staging_licenciamento_stu_v1") }}
where data < "{{ var('data_inicio_dbstu') }}"
