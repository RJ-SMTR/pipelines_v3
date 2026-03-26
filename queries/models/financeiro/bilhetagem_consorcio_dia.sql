{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

-- depends_on: {{ ref("bilhetagem_consorcio_operador_dia") }}
select
    o.data_ordem,
    o.id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
    dc.id_consorcio,
    dc.consorcio,
    o.id_ordem_pagamento as id_ordem_pagamento,
    o.qtd_debito as quantidade_transacao_debito,
    o.valor_debito,
    o.qtd_vendaabordo as quantidade_transacao_especie,
    o.valor_vendaabordo as valor_especie,
    o.qtd_gratuidade as quantidade_transacao_gratuidade,
    o.valor_gratuidade,
    o.qtd_integracao as quantidade_transacao_integracao,
    o.valor_integracao,
    o.qtd_rateio_credito as quantidade_transacao_rateio_credito,
    o.valor_rateio_credito as valor_rateio_credito,
    o.qtd_rateio_debito as quantidade_transacao_rateio_debito,
    o.valor_rateio_debito as valor_rateio_debito,
    (
        o.qtd_debito + o.qtd_vendaabordo + o.qtd_gratuidade + o.qtd_integracao
    ) as quantidade_total_transacao,
    o.valor_bruto as valor_total_transacao_bruto,
    o.valor_taxa as valor_desconto_taxa,
    o.valor_liquido as valor_total_transacao_liquido,
    '{{ var("version") }}' as versao
from {{ ref("staging_ordem_pagamento_consorcio") }} o
left join {{ ref("consorcios") }} dc on o.id_consorcio = dc.id_consorcio_jae
{% if is_incremental() %}
    where
        date(o.data) between date("{{var('date_range_start')}}") and date(
            "{{var('date_range_end')}}"
        )
{% endif %}
