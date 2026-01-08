{{
    config(
        materialized="table",
    )
}}


select
    d.id_operadora,
    cpj.nm_contato as contato,
    cpj.nr_ramal as ramal,
    coalesce(cpj.nr_telefone, c.telefone) as telefone,
    coalesce(cpj.tx_email, c.email) as email
from {{ ref("cliente_jae") }} as c
left join
    {{ ref("staging_contato_pessoa_juridica") }} cpj on c.id_cliente = cpj.cd_cliente
join {{ ref("staging_operadora_transporte") }} as ot on ot.cd_cliente = c.id_cliente
join {{ ref("operadoras") }} d on d.id_operadora_jae = ot.cd_operadora_transporte
