{{ config(materialized="ephemeral") }}

SELECT
  data,
  id_viagem,
  id_veiculo,
  servico,
  id_validador,
  tipo_viagem,
  modo,
  tecnologia_apurada,
  tecnologia_remunerada,
  sentido,
  distancia_planejada,
  quantidade_transacao,
  quantidade_transacao_riocard,
  percentual_estado_equipamento_aberto,
  indicador_estado_equipamento_aberto,
  datetime_partida_bilhetagem,
  datetime_partida,
  datetime_chegada,
  current_datetime("America/Sao_Paulo") AS datetime_ultima_atualizacao
FROM {{ ref("aux_viagem_transacao") }}
