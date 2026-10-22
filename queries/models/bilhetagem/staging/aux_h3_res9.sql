{{ config(materialized="table") }}

select tile_id, st_geogfromtext(geometry) as geometry
from {{ source("br_rj_riodejaneiro_geo", "h3_res9") }}
