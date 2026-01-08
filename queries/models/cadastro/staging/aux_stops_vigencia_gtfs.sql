{{
  config(
    materialized="ephemeral",
  )
}}

WITH stops_rn AS (
  SELECT
    stop_id AS id_servico,
    stop_code AS servico,
    stop_name AS descricao_servico,
    stop_lat AS latitude,
    stop_lon AS longitude,
    feed_start_date AS inicio_vigencia,
    feed_end_date AS fim_vigencia,
    LAG(feed_end_date) OVER (PARTITION BY stop_id ORDER BY feed_start_date) AS feed_end_date_anterior,
    ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY feed_start_date DESC) AS rn
  FROM
    {{ ref("stops_gtfs") }}
  WHERE
    location_type = '1'
),
stops_agrupada AS (
  SELECT
    id_servico,
    inicio_vigencia,
    servico,
    descricao_servico,
    IFNULL(fim_vigencia, CURRENT_DATE("America/Sao_Paulo")) AS fim_vigencia,
    SUM(
      CASE
        WHEN feed_end_date_anterior IS NULL OR feed_end_date_anterior <> DATE_SUB(inicio_vigencia, INTERVAL 1 DAY) THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY id_servico ORDER BY inicio_vigencia) AS group_id
  FROM
    stops_rn
),
vigencia AS (
  SELECT
    id_servico,
    MIN(inicio_vigencia) AS inicio_vigencia,
    MAX(fim_vigencia) AS fim_vigencia
  FROM
    stops_agrupada
  GROUP BY
    id_servico,
    group_id
)
SELECT
  id_servico,
  r.servico,
  r.descricao_servico,
  r.latitude,
  r.longitude,
  v.inicio_vigencia,
  CASE
    WHEN v.fim_vigencia != CURRENT_DATE("America/Sao_Paulo") THEN v.fim_vigencia
  END AS fim_vigencia,
  'stops' AS tabela_origem_gtfs,
FROM
 vigencia v
JOIN
(
  SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude
  FROM
    stops_rn
  WHERE
    rn = 1
) r
USING(id_servico)
