{{ config(materialized="ephemeral") }}

WITH
status AS (SELECT * FROM {{ ref("aux_viagem_status") }}),

temperatura AS (
  SELECT
    data,
    id_viagem,
    indicador_detectado_ar_inoperante,
    indicador_regularidade_ar_condicionado_viagem
  FROM {{ ref("aux_viagem_regularidade_temperatura") }}
),

bilhetagem AS (
  SELECT
    data,
    id_viagem,
    indicador_sem_transacao,
    indicador_sem_transacao_tipo,
    indicador_validador_fechado,
    indicador_validador_associado_incorretamente,
    indicador_estado_equipamento_aberto
  FROM {{ ref("aux_viagem_transacao") }}
)

SELECT
  s.data,
  s.id_viagem,
  s.datetime_partida,
  s.datetime_chegada,
  s.tipo_execucao_viagem,
  s.id_veiculo,
  s.placa,
  s.ano_fabricacao,
  s.servico,
  s.sentido,
  s.shape_id,
  s.distancia_planejada,
  s.modo,
  s.tipo_dia,
  s.tipo_dia_oferta,
  s.sistema,
  s.tipo_viagem,
  s.tecnologia_apurada,
  s.tecnologia_remunerada,
  s.menor_tecnologia_permitida,
  s.maior_tecnologia_permitida,
  s.lote,
  s.faixa_horaria_inicio,
  s.faixa_horaria_fim,
  s.indicador_viagem_apurada,
  s.indicadores,
  b.indicador_estado_equipamento_aberto,
  cast(s.distancia_planejada AS float64) AS km_programada,
  cast(
    if(
      coalesce(s.tipo_execucao_viagem, completa) = incompleta,
      least(
        coalesce(s.km_percorrida_gps, 0),
        coalesce(cast(s.distancia_planejada AS float64), 0)
      ),
      cast(s.distancia_planejada AS float64)
    ) AS float64
  ) AS km_percorrida,
  coalesce(s.indicador_nao_licenciado, false) AS indicador_nao_licenciado,
  coalesce(s.indicador_nao_vistoriado, false) AS indicador_nao_vistoriado,
  coalesce(s.indicador_lacrado, false) AS indicador_lacrado,
  coalesce(
    s.indicador_nao_autorizado_capacidade, false
  ) AS indicador_nao_autorizado_capacidade,
  coalesce(
    s.indicador_autuado_ar_inoperante, false
  ) AS indicador_autuado_ar_inoperante,
  coalesce(
    s.indicador_autuado_alterar_itinerario, false
  ) AS indicador_autuado_alterar_itinerario,
  coalesce(
    s.indicador_autuado_vista_inoperante, false
  ) AS indicador_autuado_vista_inoperante,
  coalesce(
    s.indicador_autuado_nao_atender_parada, false
  ) AS indicador_autuado_nao_atender_parada,
  coalesce(
    s.indicador_autuado_nao_concluir_itinerario, false
  ) AS indicador_autuado_nao_concluir_itinerario,
  coalesce(
    s.indicador_registrado_ar_inoperante, false
  ) AS indicador_registrado_ar_inoperante,
  coalesce(
    t.indicador_detectado_ar_inoperante, false
  ) AS indicador_detectado_ar_inoperante,
  coalesce(
    t.indicador_regularidade_ar_condicionado_viagem, true
  ) AS indicador_regularidade_ar_condicionado_viagem,
  coalesce(b.indicador_sem_transacao, false) AS indicador_sem_transacao,
  coalesce(b.indicador_sem_transacao_tipo, false)
    AS indicador_sem_transacao_tipo,
  coalesce(b.indicador_validador_fechado, false) AS indicador_validador_fechado,
  coalesce(
    b.indicador_validador_associado_incorretamente, false
  ) AS indicador_validador_associado_incorretamente
FROM status AS s
LEFT JOIN temperatura AS t ON s.data = t.data AND s.id_viagem = t.id_viagem
LEFT JOIN bilhetagem AS b USING (data, id_viagem)
