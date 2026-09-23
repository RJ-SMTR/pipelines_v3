{{
    config(
        materialized="view",
    )
}}

select distinct
    vp.servico as trip_short_name,
    vp.shape_id,
    vp.data,
    sg.shape,
    vp.feed_start_date as data_versao,
    vp.vista,
    vp.sentido
from {{ ref("viagem_planejada_planejamento_dia") }} as vp
left join
    {{ ref("shapes_geom_planejamento") }} as sg
    on vp.feed_start_date = sg.feed_start_date
    and vp.shape_id = sg.shape_id
where
    vp.data between date_sub(
        current_date("America/Sao_Paulo"), interval 8 day
    ) and current_date("America/Sao_Paulo")
