{% test transacao_valor_ordem_completa(model) %}
    {% set ref_model = ref("transacao_valor_ordem") %}

    {% if execute %}
        {% set partitions_query %}
            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
            from
                `{{ ref_model.database }}.{{ ref_model.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ ref_model.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{ var('date_range_start') }}") and (datetime("{{ var('date_range_end') }}"))

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}

    with
        transacao_valor_ordem as (
            select id_ordem_pagamento_consorcio_operador_dia
            from {{ ref_model }}
            where
                {% if partitions | length > 0 %}
                    data_ordem in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}
        ),
        ordem_pagamento as (
            select id_ordem_pagamento_consorcio_operador_dia
            from {{ ref("bilhetagem_consorcio_operador_dia") }}
            where
                {% if partitions | length > 0 %}
                    data_ordem in ({{ partitions | join(", ") }})
                    and quantidade_transacao_debito
                    + quantidade_transacao_integracao
                    + quantidade_transacao_rateio_credito
                    + quantidade_transacao_rateio_debito
                    > 0
                {% else %} false
                {% endif %}
        )
    select *
    from ordem_pagamento
    where
        id_ordem_pagamento_consorcio_operador_dia not in (
            select id_ordem_pagamento_consorcio_operador_dia from transacao_valor_ordem
        )

{% endtest %}
