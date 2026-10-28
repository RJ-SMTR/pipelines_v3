{{ config(alias="gps_onibus") }}

select
    "Ônibus" as modo,
    g.sistema,
    g.datetime_gps,
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
    g.fonte_gps,
    g.versao
from {{ ref("view_gps_onibus") }} g
