{{ config(materialized="view", alias="garagem_polygon") }}

select wkt, nm_municip, cd_geocmu
from {{ source("br_rj_riodejaneiro_geo", "garagens_polygon") }}
