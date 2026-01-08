{{
    config(
        materialized="table",
    )
}}

select
    inicio_vigencia,
    fim_vigencia,
    endereco,
    bairro,
    geometry_wkt,
    st_geogfromtext(geometry_wkt, make_valid => true) as geometry
from {{ ref("staging_garagem") }}
