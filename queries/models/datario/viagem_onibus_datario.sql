{{ config(alias="viagem_onibus") }}
select
    data,
    consorcio,
    tipo_dia,
    id_empresa,
    id_veiculo,
    id_viagem,
    servico_realizado as servico,
    shape_id,
    sentido,
    datetime_partida,
    datetime_chegada,
    tempo_viagem,
    distancia_planejada,
    perc_conformidade_shape,
    perc_conformidade_registros,
    versao_modelo
from {{ ref("viagem_completa") }}
