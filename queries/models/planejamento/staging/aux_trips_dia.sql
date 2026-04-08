{{ config(materialized="ephemeral") }}

select c.data, c.tipo_dia, c.subtipo_dia, c.tipo_os, t.* except (tipo_dia, tipo_os)
from {{ ref("calendario") }} c
join
    {{ ref("aux_trips") }} t
    on c.feed_start_date = t.feed_start_date
    and c.feed_version = t.feed_version
    and t.service_id in unnest(c.service_ids)
    and c.tipo_dia = t.tipo_dia
    and c.tipo_os = t.tipo_os
