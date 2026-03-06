SELECT
  *
FROM
  {{ ref("shapes_geom_planejamento") }}
WHERE
  feed_start_date = '{{ var("data_versao_gtfs") }}'