{{ config(alias="gps_brt") }}

with
    linhas_sigmob as (
        select distinct linha_gtfs
        from {{ source("br_rj_riodejaneiro_sigmob", "shapes_geom") }}
        where
            id_modal_smtr in ({{ var("brt_id_modal_smtr") | join(", ") }})
            and data_versao = date("{{ var('versao_fixa_sigmob') }}")
    )
select
    "BRT" as modo,
    g.timestamp_gps,
    g.data,
    g.hora,
    g.id_veiculo,
    g.servico,
    g.latitude,
    g.longitude,
    case
        when g.velocidade_estimada_10_min < {{ var("velocidade_limiar_parado") }}
        then false
        else true
    end as flag_em_movimento,
    case
        when g.status = 'Parado terminal'
        then 'terminal'
        when g.status = 'Parado garagem'
        then 'garagem'
    end as tipo_parada,
    s.linha_gtfs is not null as flag_linha_existe_sigmob,
    g.velocidade_instantanea,
    g.velocidade_estimada_10_min,
    g.distancia,
    g.versao
from {{ ref("view_gps_brt_completo") }} g
left join linhas_sigmob s on g.servico = s.linha_gtfs
