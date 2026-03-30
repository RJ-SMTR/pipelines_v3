{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endset %}

{% set transacao_ordem = ref("staging_transacao_ordem") %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}

            SELECT DISTINCT
                CONCAT("'", DATE(data_transacao), "'") AS data_transacao
            FROM
                {{ transacao_ordem }}
            WHERE
                {{ incremental_filter }}

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% endif %}
{% endif %}

with
    staging as (
        select
            date(data_transacao) as data,
            id as id_transacao,
            id_ordem_ressarcimento as id_ordem_pagamento_servico_operador_dia,
            data_transacao as datetime_transacao,
            data_processamento as datetime_processamento,
            timestamp_captura as datetime_captura
        from {{ transacao_ordem }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    new_data as (
        select
            s.data,
            s.id_transacao,
            s.datetime_transacao,
            s.datetime_processamento,
            o.data_ordem,
            id_ordem_pagamento_servico_operador_dia,
            o.id_ordem_pagamento_consorcio_operador_dia,
            o.id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento,
            s.datetime_captura
        from staging s
        join
            {{ ref("bilhetagem_servico_operador_dia") }} o using (
                id_ordem_pagamento_servico_operador_dia
            )
    ),
    complete_partitions as (
        select *, 0 as priority
        from new_data
        {% if is_incremental() and partitions | length > 0 %}
            union all
            select *, 1 as priority
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    )
select * except (rn, priority)
from
    (
        select
            *,
            row_number() over (
                partition by id_transacao order by datetime_captura desc, priority
            ) as rn
        from complete_partitions
    )
where rn = 1
