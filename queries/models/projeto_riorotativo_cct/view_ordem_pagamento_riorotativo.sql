with
    guardador as (
        select
            data_ordem,
            id_ordem_pagamento_guardador_veiculo_dia as id_ordem_pagamento,
            cpf_guardador_veiculo as documento,
            'CPF' as tipo_documento,
            quantidade_verificacao_valida,
            valor_repasse_guardador_veiculo as valor_repasse,
            datetime_inclusao,
            datetime_ultima_atualizacao
        from {{ ref("ordem_pagamento_guardador_veiculo_dia_riorotativo") }}
    ),
    entidade as (
        select
            data_ordem,
            id_ordem_pagamento_entidade_dia as id_ordem_pagamento,
            cnpj_entidade as documento,
            'CNPJ' as tipo_documento,
            quantidade_verificacao_valida,
            valor_repasse_entidade as valor_repasse,
            datetime_inclusao,
            datetime_ultima_atualizacao
        from {{ ref("ordem_pagamento_entidade_dia_riorotativo") }}
    ),
    union_guardador_entidade as (
        select *
        from guardador
        union all
        select *
        from entidade
    )
select *
from union_guardador_entidade
