{{ config(materialized="table") }}

select servico, empresa, fase, data
from {{ ref("staging_servico_contrato_abreviado") }}
where confirmacao_cgr
