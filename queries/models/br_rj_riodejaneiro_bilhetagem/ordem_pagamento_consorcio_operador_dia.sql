{{
    config(
        materialized="view",
    )
}}

select *
from {{ ref("bilhetagem_consorcio_operador_dia") }}
