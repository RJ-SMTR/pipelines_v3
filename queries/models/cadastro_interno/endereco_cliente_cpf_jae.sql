{{
    config(
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 50000000},
        },
        unique_key="id_cliente_sequencia",
    )
}}

{% set endereco_cliente_jae = ref("endereco_cliente_jae") %}


select
    cast(c.documento as integer) as cpf_particao,
    c.documento as cpf,
    e.* except (id_cliente_particao)
from {{ endereco_cliente_jae }} e
join {{ ref("cliente_jae") }} c using (id_cliente)
where c.tipo_documento = 'CPF'
