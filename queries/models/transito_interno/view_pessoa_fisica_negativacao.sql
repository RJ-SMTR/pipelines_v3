{{ config(materialized="view") }}

select
    data,
    data_inclusao,
    data_baixa,
    nome,
    cpf,
    endereco,
    bairro,
    cidade,
    cep,
    estado,
    contrato,
    datavencimento,
    datavenda,
    valor
from {{ ref("aux_autuacao_negativacao") }}
order by data, id_auto_infracao
