{{ config(materialized="ephemeral") }}

SELECT
  data,
  id_viagem,
  id_veiculo,
  servico,
  id_validador,
  CASE
    WHEN
      tipo_viagem NOT IN (
        "Licenciado com ar e não autuado", "Licenciado sem ar e não autuado"
      )
      THEN tipo_viagem
    WHEN indicador_sem_transacao_tipo
      THEN
        CASE
          WHEN
            data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
            AND NOT indicador_sem_transacao
            AND indicador_estado_equipamento_aberto
            THEN tipo_viagem
          ELSE "Sem transação"
        END
    WHEN indicador_validador_fechado
      THEN "Validador fechado"
    WHEN indicador_validador_associado_incorretamente
      THEN "Validador associado incorretamente"
    ELSE tipo_viagem
  END AS tipo_viagem,
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
FROM {{ ref("eph_viagem_transacao") }}
