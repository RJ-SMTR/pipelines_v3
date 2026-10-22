{{ config(materialized="ephemeral") }}

select
    feed_start_date,
    feed_version,
    servico,
    tipo_os,
    evento,
    case when sentido = 'V' then '1' else '0' end as direction_id,
    extensao
from
    (
        select
            feed_start_date,
            feed_version,
            servico,
            tipo_os,
            evento,
            extensao_ida,
            extensao_volta
        from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
    )
    unpivot ((extensao) for sentido in ((extensao_ida) as 'I', (extensao_volta) as 'V'))
