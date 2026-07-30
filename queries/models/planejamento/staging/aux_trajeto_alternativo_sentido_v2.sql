{{ config(materialized="ephemeral") }}

select
    feed_start_date,
    feed_version,
    servico,
    tipo_os,
    evento,
    case when left(sentido, 1) = 'V' then '1' else '0' end as direction_id,
    extensao
from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
