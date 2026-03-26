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

with
    aux_servico as (
        select
            date(datetime_inicio_validade) as data_inicio_validade,
            date(datetime_fim_validade) as data_fim_validade,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,

        from {{ ref("aux_servico_jae") }}
        qualify
            row_number() over (
                partition by date(datetime_inicio_validade), id_servico_jae
                order by data_inicio_validade desc
            )
            = 1
    ),
    ordem_pagamento as (
        select
            r.data_ordem,
            r.id_ordem_ressarcimento as id_ordem_pagamento_servico_operador_dia,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            l.id_servico_jae,
            l.servico_jae,
            l.descricao_servico_jae,
            r.id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia,
            r.id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
            r.id_ordem_pagamento as id_ordem_pagamento,
            rat.id_ordem_rateio as id_ordem_rateio,
            r.qtd_debito as quantidade_transacao_debito,
            r.valor_debito,
            r.qtd_vendaabordo as quantidade_transacao_especie,
            r.valor_vendaabordo as valor_especie,
            r.qtd_gratuidade as quantidade_transacao_gratuidade,
            r.valor_gratuidade,
            r.qtd_integracao as quantidade_transacao_integracao,
            r.valor_integracao,
            coalesce(
                rat.qtd_rateio_compensacao_credito_total, r.qtd_rateio_credito
            ) as quantidade_transacao_rateio_credito,
            coalesce(
                rat.valor_rateio_compensacao_credito_total, r.valor_rateio_credito
            ) as valor_rateio_credito,
            coalesce(
                rat.qtd_rateio_compensacao_debito_total, r.qtd_rateio_debito
            ) as quantidade_transacao_rateio_debito,
            coalesce(
                rat.valor_rateio_compensacao_debito_total, r.valor_rateio_debito
            ) as valor_rateio_debito,
            (
                r.qtd_debito + r.qtd_vendaabordo + r.qtd_gratuidade + r.qtd_integracao
            ) as quantidade_total_transacao,
            r.valor_bruto as valor_total_transacao_bruto,
            r.valor_taxa as valor_desconto_taxa,
            r.valor_liquido as valor_total_transacao_liquido
        from {{ ref("staging_ordem_ressarcimento") }} r
        left join
            {{ ref("staging_ordem_rateio") }} rat using (
                data_ordem, id_consorcio, id_operadora, id_linha
            )
        left join {{ ref("operadoras") }} as do on r.id_operadora = do.id_operadora_jae
        left join {{ ref("consorcios") }} as dc on r.id_consorcio = dc.id_consorcio_jae
        left join
            aux_servico l
            on r.id_linha = l.id_servico_jae
            and r.data_ordem >= l.data_inicio_validade
            and (r.data_ordem < l.data_fim_validade or l.data_fim_validade is null)
        {% if is_incremental() %}
            where
                date(r.data) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
        {% endif %}
    )
select
    data_ordem,
    id_ordem_pagamento_servico_operador_dia,
    id_consorcio,
    consorcio,
    id_operadora,
    operadora,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    id_ordem_pagamento_consorcio_operador_dia,
    id_ordem_pagamento_consorcio_dia,
    id_ordem_pagamento,
    id_ordem_rateio,
    quantidade_transacao_debito,
    valor_debito,
    quantidade_transacao_especie,
    valor_especie,
    quantidade_transacao_gratuidade,
    valor_gratuidade,
    quantidade_transacao_integracao,
    valor_integracao,
    quantidade_transacao_rateio_credito,
    valor_rateio_credito,
    quantidade_transacao_rateio_debito,
    valor_rateio_debito,
    quantidade_total_transacao,
    valor_total_transacao_bruto
    + valor_rateio_debito
    + valor_rateio_credito as valor_total_transacao_bruto,
    valor_desconto_taxa,
    valor_total_transacao_liquido
    + valor_rateio_debito
    + valor_rateio_credito as valor_total_transacao_liquido,
    '{{ var("version") }}' as versao
from ordem_pagamento o
