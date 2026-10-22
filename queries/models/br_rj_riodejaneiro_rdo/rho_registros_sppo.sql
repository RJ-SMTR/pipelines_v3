{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_transacao",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set staging_table = ref("staging_rho_registros_sppo") %}
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

with
    rho_new as (
        select
            data_transacao,
            hora_transacao,
            data_processamento,
            data_particao as data_arquivo_rho,
            linha as servico_riocard,
            linha_rcti as linha_riocard,
            operadora,
            total_pagantes_cartao as quantidade_transacao_cartao,
            total_pagantes_especie as quantidade_transacao_especie,
            total_gratuidades as quantidade_transacao_gratuidade,
            registro_processado,
            timestamp_captura as datetime_captura
        from {{ staging_table }}
        {% if is_incremental() %}
            where
                ano between extract(
                    year from date("{{ var('date_range_start') }}")
                ) and extract(year from date("{{ var('date_range_end') }}"))
                and mes between extract(
                    month from date("{{ var('date_range_start') }}")
                ) and extract(month from date("{{ var('date_range_end') }}"))
                and dia between extract(
                    day from date("{{ var('date_range_start') }}")
                ) and extract(day from date("{{ var('date_range_end') }}"))
        {% endif %}
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
    ),
    -- Deduplica os dados com base na data e hora da transacao, linha, linha_rcti e
    -- operadora
    rho_rn as (
        select
            *,
            row_number() over (
                partition by
                    data_transacao,
                    hora_transacao,
                    data_arquivo_rho,
                    servico_riocard,
                    linha_riocard,
                    operadora
                order by datetime_captura desc
            ) as rn
        from rho_complete_partitions
    )
select * except (rn)
from rho_rn
where rn = 1
