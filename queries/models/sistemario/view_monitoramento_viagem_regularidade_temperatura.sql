{{ config(materialized="view") }}

SELECT
  data,
  id_viagem,
  id_veiculo,
  datetime_partida,
  datetime_chegada,
  servico,
  sentido,
  indicador_detectado_ar_inoperante,
  indicador_regularidade_ar_condicionado_viagem,
  indicadores,
  datetime_ultima_atualizacao
FROM {{ ref("viagem_valida_temperatura") }}
