{{
    config(
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 50000000},
        },
    )
}}

{% set cliente_jae = ref("cliente_jae") %}

select
    cast(documento as integer) as cpf_particao,
    documento as cpf,
    * except (
        documento,
        nome_social,
        id_cliente_particao,
        documento_alternativo,
        email,
        tipo_documento,
        tipo_pessoa
    )
from {{ cliente_jae }}
where tipo_documento = 'CPF'
