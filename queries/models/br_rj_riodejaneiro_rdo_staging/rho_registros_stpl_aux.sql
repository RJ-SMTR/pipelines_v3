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

-- Tabela auxiliar para manter os dados com os mesmos identificadores:
-- data e hora de transacao, linha e operadora desagregados antes de somar na tabela
-- final
-- Foi criada para não ter o risco de somar os dados do mesmo arquivo mais de uma vez
{% set incremental_filter %}
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


{% set staging_table = ref("staging_rho_registros_stpl") %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            SELECT DISTINCT
                CONCAT("'", data_transacao, "'")
            FROM
                {{ staging_table }}
            WHERE
                {{ incremental_filter }}
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% endif %}
{% endif %}


with
    rho_new as (
        select
            data_transacao,
            hora_transacao,
            data_particao as data_arquivo_rho,
            trim(linha) as servico_riocard,
            trim(operadora) as operadora,
            total_pagantes as quantidade_transacao_pagante,
            total_gratuidades as quantidade_transacao_gratuidade,
            timestamp_captura as datetime_captura
        from {{ staging_table }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    rho_complete_partitions as (
        select *
        from rho_new

        {% if is_incremental() and partition_list | length > 0 %}

            union all

            select *
            from {{ this }}
            where data_transacao in ({{ partition_list | join(", ") }})
        {% endif %}
    )
select
    data_transacao,
    hora_transacao,
    data_arquivo_rho,
    servico_riocard,
    operadora,
    quantidade_transacao_pagante,
    quantidade_transacao_gratuidade,
    datetime_captura
from
    (
        select
            *,
            row_number() over (
                partition by
                    data_transacao,
                    hora_transacao,
                    data_arquivo_rho,
                    servico_riocard,
                    operadora
                order by datetime_captura desc
            ) as rn
        from rho_complete_partitions
    )
where rn = 1
