{{
  config(
    materialized="ephemeral",
  )
}}

SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    inicio_vigencia,
    fim_vigencia,
    tabela_origem_gtfs,
    '{{ var("version") }}' as versao
FROM
    {{ ref('aux_routes_vigencia_gtfs') }}

UNION ALL

SELECT
    id_servico,
    servico,
    descricao_servico,
    latitude,
    longitude,
    inicio_vigencia,
    fim_vigencia,
    tabela_origem_gtfs,
    '{{ var("version") }}' as versao
FROM
    {{ ref('aux_stops_vigencia_gtfs') }}

