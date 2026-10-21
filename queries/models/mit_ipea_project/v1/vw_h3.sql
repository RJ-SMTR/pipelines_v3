-- H3 Resolution 9 view, convert geometry column to GEOGRAPHY type
select
    tile_id,
    resolution,
    parent_id,
    st_geogfromtext(geometry_wkt) as geometry,  -- Convert string in GEOGRAPHY value
    st_centroid(st_geogfromtext(geometry_wkt)) as centroid
from `rj-smtr.br_rj_riodejaneiro_geo.h3_res9`
