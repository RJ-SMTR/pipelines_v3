{{ config(alias="gps_brt") }}

select
    modo,
    timestamp_gps,
    data,
    hora,
    id_veiculo,
    servico,
    latitude,
    longitude,
    flag_em_movimento,
    tipo_parada,
    flag_linha_existe_sigmob,
    velocidade_instantanea,
    velocidade_estimada_10_min,
    distancia,
    versao
from {{ ref("gps_brt") }}
