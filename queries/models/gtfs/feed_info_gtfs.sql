{{config(
  materialized='incremental',
  partition_by = { 'field' :'feed_start_date',
  'data_type' :'date',
  'granularity': 'day' },
  alias = 'feed_info',
  unique_key = 'feed_start_date',
  incremental_strategy = 'insert_overwrite'
)}}

WITH feed_info AS (
  SELECT
    SAFE_CAST(timestamp_captura AS STRING) AS feed_version,
    SAFE_CAST(data_versao AS DATE) AS feed_start_date,
    NULL AS feed_end_date,
    SAFE_CAST(feed_publisher_name AS STRING) feed_publisher_name,
    SAFE_CAST(JSON_VALUE(content, '$.feed_publisher_url') AS STRING) feed_publisher_url,
    SAFE_CAST(JSON_VALUE(content, '$.feed_lang') AS STRING) feed_lang,
    SAFE_CAST(JSON_VALUE(content, '$.default_lang') AS STRING) default_lang,
    SAFE_CAST(JSON_VALUE(content, '$.feed_contact_email') AS STRING) feed_contact_email,
    SAFE_CAST(JSON_VALUE(content, '$.feed_contact_url') AS STRING) feed_contact_url,
    CURRENT_DATETIME("America/Sao_Paulo") AS feed_update_datetime,
    '{{ var("version") }}' AS versao_modelo
  FROM
    {{ source(
      'br_rj_riodejaneiro_gtfs_staging',
      'feed_info'
    ) }}
  {% if is_incremental() %}
    WHERE
      data_versao =  '{{ var("data_versao_gtfs") }}'
    UNION ALL
      SELECT
        *
      FROM
        {{ this }}
      WHERE
        feed_start_date != DATE('{{ var("data_versao_gtfs") }}')
  {% endif %}
  )
  SELECT
    feed_version,
    feed_start_date,
    DATE_SUB(LEAD(DATE(feed_version)) OVER (ORDER BY feed_version), INTERVAL 1 DAY) AS feed_end_date,
    feed_publisher_name,
    feed_publisher_url,
    feed_lang,
    default_lang,
    feed_contact_email,
    feed_contact_url,
    feed_update_datetime,
    versao_modelo
  FROM
    feed_info