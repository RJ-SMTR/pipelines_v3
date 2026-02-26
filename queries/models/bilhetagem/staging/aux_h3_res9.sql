{{
  config(materialized="table")
}}

SELECT
  tile_id,
  ST_GEOGFROMTEXT(geometry) AS geometry
FROM
  {{ source("br_rj_riodejaneiro_geo", "h3_res9") }}