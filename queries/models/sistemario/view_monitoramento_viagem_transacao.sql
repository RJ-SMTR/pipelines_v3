{{ config(materialized="view") }}

SELECT
  data,
  id_viagem,
  id_veiculo,
  servico,
  id_validador,
  indicador_sem_transacao,
  indicador_sem_transacao_tipo,
  indicador_validador_fechado,
  indicador_validador_associado_incorretamente,
  indicador_estado_equipamento_aberto,
  quantidade_transacao,
  quantidade_transacao_riocard,
  percentual_estado_equipamento_aberto,
  datetime_partida,
  datetime_chegada,
  datetime_ultima_atualizacao
FROM {{ ref("viagem_valida_bilhetagem") }}
