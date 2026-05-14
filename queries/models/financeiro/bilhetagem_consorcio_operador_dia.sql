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
-- depends_on: {{ ref("bilhetagem_servico_operador_dia") }}
{% set ordem_pagamento_consorcio_operadora_staging = ref(
    "staging_ordem_pagamento_consorcio_operadora"
) %}
{% set aux_retorno_ordem_pagamento = ref("aux_retorno_ordem_pagamento") %}

{% if execute %}
    {% if is_incremental() %}
        -- Verifica as ordens de pagamento capturadas
        {% set partitions_query %}
            SELECT DISTINCT
            CONCAT("'", DATE(data_ordem), "'") AS data_ordem
            FROM
            {{ ordem_pagamento_consorcio_operadora_staging }}
            WHERE
                DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
                AND timestamp_captura > DATETIME("{{var('date_range_start')}}")
                AND timestamp_captura <= DATETIME("{{var('date_range_end')}}")
        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

        {% set paid_orders_query %}
            SELECT
                CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
            FROM
                `{{ aux_retorno_ordem_pagamento.database }}.{{ aux_retorno_ordem_pagamento.schema }}.INFORMATION_SCHEMA.PARTITIONS`
                {# `rj-smtr.controle_financeiro_staging.INFORMATION_SCHEMA.PARTITIONS` #}
            WHERE
                table_name = "{{ aux_retorno_ordem_pagamento.identifier }}"
                AND partition_id != "__NULL__"
                AND DATETIME(last_modified_time, "America/Sao_Paulo") BETWEEN DATETIME("{{var('date_range_start')}}") AND (DATETIME("{{var('date_range_end')}}"))
        {% endset %}

        {% set paid_orders = run_query(paid_orders_query).columns[0].values() %}
    {% endif %}
{% endif %}


with
    pagamento as (
        select data_pagamento, data_ordem, id_consorcio, id_operadora, valor_pago
        from {{ aux_retorno_ordem_pagamento }}
        -- `rj-smtr.controle_financeiro_staging.aux_retorno_ordem_pagamento`
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %}
                    data_ordem in ({{ partitions | join(", ") }})
                {% else %} data_ordem = '2000-01-01'
                {% endif %}
                {% if paid_orders | length > 0 %}
                    or data_ordem in ({{ paid_orders | join(", ") }})
                {% endif %}

        {% endif %}
    ),
    ordem_pagamento as (
        select
            o.data_ordem,
            o.id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            op.id_ordem_pagamento as id_ordem_pagamento,
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
            o.valor_liquido as valor_total_transacao_liquido_ordem,
            o.timestamp_captura as datetime_captura,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from {{ ordem_pagamento_consorcio_operadora_staging }} o
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.ordem_pagamento_consorcio_operadora` o
        join
            {{ ref("staging_ordem_pagamento") }} op
            {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.ordem_pagamento` op #}
            on o.data_ordem = op.data_ordem
        left join {{ ref("operadoras") }} do on o.id_operadora = do.id_operadora_jae
        {# `rj-smtr.cadastro.operadoras` do on o.id_operadora = do.id_operadora_jae #}
        left join {{ ref("consorcios") }} dc on o.id_consorcio = dc.id_consorcio_jae
        {# `rj-smtr.cadastro.consorcios` dc on o.id_consorcio = dc.id_consorcio_jae #}
        {% if is_incremental() %}
            where
                date(o.data) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and o.timestamp_captura > datetime("{{var('date_range_start')}}")
                and o.timestamp_captura <= datetime("{{var('date_range_end')}}")
        {% endif %}
    ),
    ordem_pagamento_completa as (
        select *, 0 as priority
        from ordem_pagamento

        {% if is_incremental() and paid_orders | length > 0 %}
            union all

            select
                data_ordem,
                id_ordem_pagamento_consorcio_operador_dia,
                id_consorcio,
                consorcio,
                id_operadora,
                operadora,
                id_ordem_pagamento,
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
                valor_total_transacao_bruto,
                valor_desconto_taxa,
                valor_total_transacao_liquido_ordem,
                datetime_captura,
                datetime_ultima_atualizacao,
                1 as priority
            from {{ this }}
            where data_ordem in ({{ paid_orders | join(", ") }})
        {% endif %}
    ),
    ordem_valor_pagamento as (
        select
            data_ordem,
            id_ordem_pagamento_consorcio_operador_dia,
            id_consorcio,
            o.consorcio,
            id_operadora,
            o.operadora,
            o.id_ordem_pagamento,
            o.quantidade_transacao_debito,
            o.valor_debito,
            o.quantidade_transacao_especie,
            o.valor_especie,
            o.quantidade_transacao_gratuidade,
            o.valor_gratuidade,
            o.quantidade_transacao_integracao,
            o.valor_integracao,
            o.quantidade_transacao_rateio_credito,
            o.valor_rateio_credito,
            o.quantidade_transacao_rateio_debito,
            o.valor_rateio_debito,
            o.quantidade_total_transacao,
            o.valor_total_transacao_bruto,
            o.valor_desconto_taxa,
            o.valor_total_transacao_liquido_ordem,
            p.data_pagamento,
            p.valor_pago,
            o.datetime_captura,
            o.datetime_ultima_atualizacao,
            row_number() over (
                partition by data_ordem, id_consorcio, id_operadora order by priority
            ) as rn
        from ordem_pagamento_completa o
        left join pagamento p using (data_ordem, id_consorcio, id_operadora)
    )
select
    data_ordem,
    id_ordem_pagamento_consorcio_operador_dia,
    id_consorcio,
    consorcio,
    id_operadora,
    operadora,
    id_ordem_pagamento,
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
    valor_total_transacao_bruto,
    valor_desconto_taxa,
    valor_total_transacao_liquido_ordem,
    case
        when data_ordem = '2024-06-07' and id_consorcio = '2' and id_operadora = '8'
        then valor_total_transacao_liquido_ordem - 1403.4532  -- Corrigir valor pago incorretamente ao VLT na ordem do dia 2024-05-31
        else valor_total_transacao_liquido_ordem
    end as valor_total_transacao_liquido,
    data_pagamento,
    valor_pago,
    datetime_captura,
    '{{ var("version") }}' as versao,
    datetime_ultima_atualizacao
from ordem_valor_pagamento
where rn = 1
