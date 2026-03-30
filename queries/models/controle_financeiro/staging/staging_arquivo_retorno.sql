{{
  config(
    alias="arquivo_retorno"
  )
}}

SELECT
  data,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
  id,
  DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', SAFE_CAST(JSON_VALUE(content, '$.dataCaptura') AS STRING))) AS dataCaptura,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', SAFE_CAST(JSON_VALUE(content, '$.dataHoraGeracaoRetorno') AS STRING)), "America/Sao_Paulo") AS dataHoraGeracaoRetorno,
  DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', SAFE_CAST(JSON_VALUE(content, '$.dataOrdem') AS STRING))) AS dataOrdem,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', SAFE_CAST(JSON_VALUE(content, '$.dataProcessamento') AS STRING)), "America/Sao_Paulo") AS dataProcessamento,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3S%Ez', SAFE_CAST(JSON_VALUE(content, '$.dataVencimento') AS STRING))) AS dataVencimento,
  SAFE_CAST(JSON_VALUE(content, '$.favorecido') AS STRING) AS favorecido,
  SAFE_CAST(JSON_VALUE(content, '$.idConsorcio') AS STRING) AS idConsorcio,
  SAFE_CAST(JSON_VALUE(content, '$.idOperadora') AS STRING) AS idOperadora,
  SAFE_CAST(JSON_VALUE(content, '$.idOrdemPagamento') AS STRING) AS idOrdemPagamento,
  SAFE_CAST(JSON_VALUE(content, '$.isPago') AS BOOL) AS isPago,
  SAFE_CAST(JSON_VALUE(content, '$.nomeConsorcio') AS STRING) AS nomeConsorcio,
  SAFE_CAST(JSON_VALUE(content, '$.nomeOperadora') AS STRING) AS nomeOperadora,
  JSON_VALUE(content, '$.ocorrencias') AS ocorrencias,
  SAFE_CAST(JSON_VALUE(content, '$.valor') AS NUMERIC) AS valor,
  SAFE_CAST(JSON_VALUE(content, '$.valorRealEfetivado') AS NUMERIC) AS valorRealEfetivado
FROM
  {{ source("controle_financeiro_staging", "arquivo_retorno") }}