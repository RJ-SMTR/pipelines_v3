{{ config(alias="gps_brt") }}

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
    g.velocidade_instantanea,
    g.velocidade_estimada_10_min,
    g.distancia,
    g.versao
from {{ ref("view_gps_brt_completo") }} g
