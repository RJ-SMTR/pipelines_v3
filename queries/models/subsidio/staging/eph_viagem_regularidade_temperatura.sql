{{ config(materialized="ephemeral") }}

{% set incremental_filter %}
    data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

{% set condicao_veiculo %}
(
    (
        vt.ano_fabricacao <= 2019
        or vt.data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
    )
    and (
        not vt.indicador_temperatura_nula_viagem
        or (
            vt.data >= date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}')
            and coalesce(vr.indicador_falha_recorrente, false)
        )
    )
)
{% endset %}

WITH
viagem_temperatura AS (
  SELECT
    data,
    id_viagem,
    id_veiculo,
    datetime_partida,
    datetime_chegada,
    modo,
    ano_fabricacao,
    tecnologia_apurada,
    tecnologia_remunerada,
    tipo_viagem,
    servico,
    sentido,
    distancia_planejada,
    parse_json(indicadores_str) AS indicadores,
    safe_cast(
      json_value(
        parse_json(indicadores_str),
        '$.indicador_temperatura_variacao_viagem.valor'
      ) AS bool
    ) AS indicador_temperatura_variacao_viagem,
    safe_cast(
      json_value(
        parse_json(indicadores_str),
        '$.indicador_temperatura_transmitida_viagem.valor'
      ) AS bool
    ) AS indicador_temperatura_transmitida_viagem,
    safe_cast(
      json_value(
        parse_json(indicadores_str),
        '$.indicador_temperatura_pos_tratamento_descartada_viagem.valor'
      ) AS bool
    ) AS indicador_temperatura_pos_tratamento_descartada_viagem,
    safe_cast(
      json_value(
        parse_json(indicadores_str),
        '$.indicador_temperatura_zero_viagem.valor'
      ) AS bool
    ) AS indicador_temperatura_zero_viagem,
    safe_cast(
      json_value(
        parse_json(indicadores_str),
        '$.indicador_temperatura_nula_viagem.valor'
      ) AS bool
    ) AS indicador_temperatura_nula_viagem,
    safe_cast(
      json_value(
        parse_json(indicadores_str),
        '$.indicador_temperatura_regular_viagem.valor'
      ) AS bool
    ) AS indicador_temperatura_regular_viagem
  FROM {{ ref("eph_viagem_temperatura") }}
  WHERE {{ incremental_filter }}
),

veiculo_regularidade AS (
  SELECT
    data,
    id_veiculo,
    indicadores.indicador_falha_recorrente.valor AS indicador_falha_recorrente,
    indicadores.indicador_falha_recorrente.data_verificacao_falha
      AS data_verificacao_falha
  FROM {{ ref("veiculo_regularidade_temperatura_dia") }}
  WHERE {{ incremental_filter }}
)

SELECT
  vt.data,
  vt.id_viagem,
  vt.id_veiculo,
  vt.datetime_partida,
  vt.datetime_chegada,
  vt.modo,
  vt.ano_fabricacao,
  vt.tecnologia_apurada,
  vt.tecnologia_remunerada,
  vt.tipo_viagem,
  {{ condicao_veiculo }}
  AND (
    (
      vt.data >= date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
      AND coalesce(vr.indicador_falha_recorrente, false)
    )
    OR vt.indicador_temperatura_zero_viagem
    OR NOT vt.indicador_temperatura_transmitida_viagem
    OR NOT vt.indicador_temperatura_regular_viagem
  ) AS indicador_detectado_ar_inoperante,
  CASE
    WHEN {{ condicao_veiculo }}
      THEN
        (
          (
            vt.data < date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
            OR (
              vt.data >= date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
              AND NOT coalesce(vr.indicador_falha_recorrente, false)
            )
          )
          AND NOT vt.indicador_temperatura_zero_viagem
          AND vt.indicador_temperatura_transmitida_viagem
          AND vt.indicador_temperatura_regular_viagem
        )
    WHEN vt.indicador_temperatura_nula_viagem = true
      THEN true
  END AS indicador_regularidade_ar_condicionado_viagem,
  vr.indicador_falha_recorrente,
  vr.data_verificacao_falha,
  vt.indicadores,
  vt.servico,
  vt.sentido,
  vt.distancia_planejada
FROM viagem_temperatura AS vt
LEFT JOIN
  veiculo_regularidade AS vr
  ON vt.data = vr.data AND vt.id_veiculo = vr.id_veiculo
