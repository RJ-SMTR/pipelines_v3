{{ config(materialized="table") }}

select c.documento, c.tipo_documento, c.cnpj, j.id_cliente
from {{ ref("staging_riorotativo_credenciado") }} as c
left join {{ ref("cliente_cpf_jae") }} as j on c.documento = j.cpf
