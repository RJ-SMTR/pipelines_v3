select *
from {{ ref("shapes_geom_planejamento") }}
where feed_start_date = '{{ var("data_versao_gtfs") }}'
