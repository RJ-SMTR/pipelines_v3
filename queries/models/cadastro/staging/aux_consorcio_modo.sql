{{ config(materialized="table") }}
select *
from {{ source("cadastro_staging", "consorcio_modo") }}
