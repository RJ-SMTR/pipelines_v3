{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_transacao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set staging_table = ref("staging_rho_registros_stpl") %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            SELECT DISTINCT
                CONCAT("'", data_transacao, "'")
            FROM
                {{ staging_table }}
            WHERE
                ano BETWEEN
                    EXTRACT(YEAR FROM DATE("{{ var('date_range_start') }}"))
                    AND EXTRACT(YEAR FROM DATE("{{ var('date_range_end') }}"))
                AND mes BETWEEN
                    EXTRACT(MONTH FROM DATE("{{ var('date_range_start') }}"))
                    AND EXTRACT(MONTH FROM DATE("{{ var('date_range_end') }}"))
                AND dia BETWEEN
                    EXTRACT(DAY FROM DATE("{{ var('date_range_start') }}"))
                    AND EXTRACT(DAY FROM DATE("{{ var('date_range_end') }}"))
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}

select
    data_transacao,
    hora_transacao,
    servico_riocard,
    operadora,
    sum(quantidade_transacao_pagante) as quantidade_transacao_pagante,
    sum(quantidade_transacao_gratuidade) as quantidade_transacao_gratuidade
from {{ ref("rho_registros_stpl_aux") }}
{% if is_incremental() %}
    where
        data_transacao
        {% if partition_list | length > 0 %} in ({{ partition_list | join(", ") }})
        {% else %} = "2000-01-01"
        {% endif %}
{% endif %}
group by 1, 2, 3, 4
