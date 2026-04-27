select
    ot.cd_operadora_transporte,
    ot.cd_cliente,
    m.modo,
    ot.cd_tipo_modal,
    ot.ds_tipo_modal as modo_jae,
    -- STU considera BRT como Ônibus
    case when ot.cd_tipo_modal = '3' then 'Ônibus' else m.modo end as modo_join,
    ot.in_situacao_atividade,
    case
        when c.tipo_pessoa = 'Física'
        then 'CPF'
        when c.tipo_pessoa = 'Jurídica'
        then 'CNPJ'
    end as tipo_documento,
    c.documento,
    c.nome as nm_cliente,
    cb.cd_agencia,
    cb.cd_tipo_conta,
    cb.nm_banco,
    cb.nr_banco,
    cb.nr_conta
from {{ ref("staging_operadora_transporte") }} as ot
join {{ ref("cliente_jae") }} as c on ot.cd_cliente = c.id_cliente
left join {{ ref("staging_conta_bancaria") }} as cb on ot.cd_cliente = cb.cd_cliente
join {{ ref("modos") }} m on ot.cd_tipo_modal = m.id_modo and m.fonte = "jae"
