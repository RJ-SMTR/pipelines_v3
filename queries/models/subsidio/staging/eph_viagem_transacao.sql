{{ config(materialized="ephemeral") }}

{% if var("flow_name") == "treatment--monitoramento-temperatura" %}
    {% set interval_minutes = 120 %}
{% else %}
  {% set interval_minutes = 30 %}
{% endif %}

{% set date_range_start %}
  {% if var("flow_name") == "treatment--monitoramento-temperatura" %}
        "{{ var('date_range_start') }}"
    {% else %}
       "{{ var('start_date') }}"
    {% endif %}
{% endset %}
{% set date_range_end %}
  {% if var("flow_name") == "treatment--monitoramento-temperatura" %}
        "{{ var('date_range_end') }}"
    {% else %}
       "{{ var('end_date') }}"
    {% endif %}
{% endset %}

WITH
-- Transações Jaé
transacao AS (
  SELECT
    id_veiculo,
    servico_jae,
    datetime_transacao
  FROM {{ ref("transacao") }}
  -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
  WHERE
    data BETWEEN date({{ date_range_start }}) AND date_add(
      date({{ date_range_end }}), INTERVAL 1 DAY
    )
    AND date(datetime_processamento) - date(datetime_transacao)
    <= INTERVAL 6 DAY
    AND modo = "Ônibus"
),

-- Transações RioCard
transacao_riocard AS (
  SELECT
    id_veiculo,
    servico_jae,
    datetime_transacao
  FROM {{ ref("transacao_riocard") }}
  -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
  WHERE
    data BETWEEN date({{ date_range_start }}) AND date_add(
      date({{ date_range_end }}), INTERVAL 1 DAY
    )
    AND date(datetime_processamento) - date(datetime_transacao)
    <= INTERVAL 6 DAY
    AND modo = "Ônibus"
),

-- -- Viagens realizadas
viagem AS (
  SELECT
    data,
    id_viagem,
    id_veiculo,
    datetime_partida,
    datetime_chegada,
    modo,
    tecnologia_apurada,
    tecnologia_remunerada,
    {% if var("sistema") == "rio" %}
            cast(null as string) as tipo_viagem,
        {% else %}
      tipo_viagem,
    {% endif %}
    indicadores,
    servico,
    sentido,
    distancia_planejada
  {% if var("sistema") == "rio" %}
            from {{ ref("viagem_valida_temperatura") }}
        {% else %}
    FROM {{ ref("viagem_regularidade_temperatura") }}
  {% endif %}
  WHERE
    data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    AND (
      data BETWEEN date({{ date_range_start }}) AND date({{ date_range_end }})
      {% if target.name == "prod" %}
                    or data = date_sub(date({{ date_range_start }}), interval 1 day)
                {% endif %}
    )

  {% if target.name in ("dev", "hmg") %}
            --fmt:off
            left outer union all by name
             --fmt:on
            (
                select id_veiculo, datetime_partida, datetime_chegada
                from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
                where
                    data = date_sub(date({{ date_range_start }}), interval 1 day)
                    and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
                    and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")

                union all by name

                select id_veiculo, datetime_partida, datetime_chegada
                from `rj-smtr.monitoramento.viagem_valida`
                where
                    data = date_sub(date({{ date_range_start }}), interval 1 day)
                    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            )
        {% endif %}
),

-- Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos,
-- limitada pela viagem anterior
viagem_com_tolerancia_previa AS (
  SELECT
    v.*,
    lag(v.datetime_chegada) OVER (
      PARTITION BY v.id_veiculo ORDER BY v.datetime_partida
    ) AS viagem_anterior_chegada,
    CASE
      WHEN
        lag(v.datetime_chegada)
          OVER (
            PARTITION BY v.id_veiculo ORDER BY v.datetime_partida
          )
        IS null
        THEN
          datetime(
            timestamp_sub(
              datetime_partida, INTERVAL {{ interval_minutes }} MINUTE
            )
          )
      ELSE
        datetime(
          timestamp_add(
            greatest(
              timestamp_sub(
                datetime_partida,
                INTERVAL {{ interval_minutes }} MINUTE
              ),
              lag(v.datetime_chegada) OVER (
                PARTITION BY v.id_veiculo
                ORDER BY v.datetime_partida
              )
            ),
            INTERVAL 1 SECOND
          )
        )
    END AS datetime_partida_com_tolerancia
  FROM viagem AS v
),

-- Considera apenas as viagens realizadas no período de apuração
viagem_com_tolerancia AS (
  SELECT *
  FROM viagem_com_tolerancia_previa
  WHERE data BETWEEN date({{ date_range_start }}) AND date({{ date_range_end }})
),

-- Contagem de transações Jaé
transacao_contagem AS (
  SELECT
    v.data,
    v.id_viagem,
    count(t.datetime_transacao) AS quantidade_transacao,
    countif(
      v.servico != t.servico_jae AND t.datetime_transacao > v.datetime_partida
    ) AS quantidade_transacao_servico_divergente
  FROM transacao AS t
  INNER JOIN
    viagem_com_tolerancia AS v
    ON
      t.id_veiculo = substr(v.id_veiculo, 2)
      AND t.datetime_transacao
      BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
  GROUP BY 1, 2
),

-- Contagem de transações RioCard
transacao_riocard_contagem AS (
  SELECT
    v.data,
    v.id_viagem,
    count(tr.datetime_transacao) AS quantidade_transacao_riocard,
    countif(
      v.servico != tr.servico_jae
      AND tr.datetime_transacao > v.datetime_partida
    ) AS quantidade_transacao_riocard_servico_divergente
  FROM transacao_riocard AS tr
  INNER JOIN
    viagem_com_tolerancia AS v
    ON
      tr.id_veiculo = substr(v.id_veiculo, 2)
      AND tr.datetime_transacao
      BETWEEN v.datetime_partida_com_tolerancia AND v.datetime_chegada
  GROUP BY 1, 2
),

-- Calcula a porcentagem de estado do equipamento "ABERTO" por
-- validador e
-- viagem
estado_equipamento_perc AS (
  SELECT
    v.data,
    v.id_viagem,
    safe_cast(json_value(item, '$.id_validador') AS string) AS id_validador,
    coalesce(t.quantidade_transacao, 0) AS quantidade_transacao,
    coalesce(
      tr.quantidade_transacao_riocard, 0
    ) AS quantidade_transacao_riocard,
    coalesce(
      t.quantidade_transacao_servico_divergente, 0
    ) AS quantidade_transacao_servico_divergente,
    coalesce(
      tr.quantidade_transacao_riocard_servico_divergente, 0
    ) AS quantidade_transacao_riocard_servico_divergente,
    safe_cast(
      json_value(item, '$.percentual_estado_equipamento_aberto') AS numeric
    ) AS percentual_estado_equipamento_aberto,
    safe_cast(
      json_value(item, '$.indicador_estado_equipamento_aberto') AS bool
    ) AS indicador_estado_equipamento_aberto,
    safe_cast(
      json_value(item, '$.indicador_gps_servico_divergente') AS bool
    ) AS indicador_gps_servico_divergente
  FROM viagem AS v
  LEFT JOIN
    transacao_contagem AS t
    ON v.data = t.data AND v.id_viagem = t.id_viagem
  LEFT JOIN
    transacao_riocard_contagem AS tr
    ON
      v.data = tr.data
      AND v.id_viagem = tr.id_viagem
  LEFT JOIN
    unnest(
      json_query_array(v.indicadores, '$.indicador_validador.valores')
    )
),

validador_tipo_viagem AS (
  SELECT
    data,
    id_viagem,
    id_validador,
    CASE
      WHEN data < date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
        THEN quantidade_transacao_riocard = 0
      ELSE (quantidade_transacao_riocard = 0 AND quantidade_transacao = 0)
    END AS indicador_sem_transacao,
    indicador_estado_equipamento_aberto,
    (
      data >= date('{{ var("DATA_SUBSIDIO_V8_INICIO") }}')
      AND (
        (
          data < date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
          AND (
            quantidade_transacao_riocard = 0
            OR NOT indicador_estado_equipamento_aberto
          )
        )
        OR (
          data >= date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
          AND data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
          AND (
            (
              quantidade_transacao_riocard = 0
              AND quantidade_transacao = 0
            )
            OR NOT indicador_estado_equipamento_aberto
          )
        )
        OR (
          data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
          AND (
            quantidade_transacao_riocard = 0
            AND quantidade_transacao = 0
          )
        )
      )
    ) AS indicador_sem_transacao_tipo,
    (
      data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
      AND NOT indicador_estado_equipamento_aberto
    ) AS indicador_validador_fechado,
    (
      data >= date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
      AND (
        quantidade_transacao_riocard_servico_divergente > 0
        OR quantidade_transacao_servico_divergente > 0
        OR indicador_gps_servico_divergente
      )
    ) AS indicador_validador_associado_incorretamente
  FROM estado_equipamento_perc
),

flags_viagem AS (
  SELECT
    data,
    id_viagem,
    max(indicador_sem_transacao) AS indicador_sem_transacao,
    max(indicador_sem_transacao_tipo) AS indicador_sem_transacao_tipo,
    logical_or(indicador_validador_fechado) AS indicador_validador_fechado,
    logical_or(
      indicador_validador_associado_incorretamente
    ) AS indicador_validador_associado_incorretamente,
    CASE
      WHEN data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
        THEN max(indicador_estado_equipamento_aberto)
      ELSE min(indicador_estado_equipamento_aberto)
    END AS indicador_estado_equipamento_aberto,
    array_agg(DISTINCT id_validador IGNORE NULLS) AS id_validador
  FROM validador_tipo_viagem
  GROUP BY 1, 2
)

SELECT
  v.data,
  v.id_viagem,
  v.id_veiculo,
  v.servico,
  f.id_validador,
  v.tipo_viagem,
  f.indicador_sem_transacao,
  f.indicador_sem_transacao_tipo,
  f.indicador_validador_fechado,
  f.indicador_validador_associado_incorretamente,
  v.modo,
  v.tecnologia_apurada,
  v.tecnologia_remunerada,
  v.sentido,
  v.distancia_planejada,
  v.indicadores,
  any_value(eep.quantidade_transacao) AS quantidade_transacao,
  any_value(eep.quantidade_transacao_riocard) AS quantidade_transacao_riocard,
  CASE
    WHEN v.data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
      THEN max(eep.percentual_estado_equipamento_aberto)
    ELSE min(eep.percentual_estado_equipamento_aberto)
  END AS percentual_estado_equipamento_aberto,
  f.indicador_estado_equipamento_aberto,
  v.datetime_partida_com_tolerancia AS datetime_partida_bilhetagem,
  v.datetime_partida,
  v.datetime_chegada
FROM viagem_com_tolerancia AS v
LEFT JOIN
  estado_equipamento_perc AS eep
  ON v.data = eep.data AND v.id_viagem = eep.id_viagem
LEFT JOIN flags_viagem AS f USING (data, id_viagem)
GROUP BY all
