{{ config(materialized="ephemeral") }}

select *
from {{ ref("infracao") }}
where modo = 'ONIBUS' and placa is not null
