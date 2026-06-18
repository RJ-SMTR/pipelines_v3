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

{% set staging_table = ref("staging_rdo_registros_stpl") %}
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
    rdo_new as (
        select
            data_transacao,
            data_particao as data_arquivo_rdo,
            trim(linha) as servico_riocard,
            trim(linha_rcti) as linha_riocard,
            trim(operadora) as operadora,
            gratuidade_idoso as quantidade_transacao_gratuidade_idoso,
            gratuidade_especial as quantidade_transacao_gratuidade_especial,
            gratuidade_estudante_federal
            as quantidade_transacao_gratuidade_estudante_federal,
            gratuidade_estudante_estadual
            as quantidade_transacao_gratuidade_estudante_estadual,
            gratuidade_estudante_municipal
            as quantidade_transacao_gratuidade_estudante_municipal,
            universitario as quantidade_transacao_buc_universitario,
            buc_1a_perna as quantidade_transacao_buc_perna_1,
            buc_2a_perna as quantidade_transacao_buc_perna_2,
            buc_receita as valor_buc,
            buc_supervia_1a_perna as quantidade_transacao_buc_supervia_perna_1,
            buc_supervia_2a_perna as quantidade_transacao_buc_supervia_perna_2,
            buc_supervia_receita as valor_buc_supervia,
            buc_van_1a_perna as quantidade_transacao_buc_van_perna_1,
            buc_van_2a_perna as quantidade_transacao_buc_van_perna_2,
            buc_van_receita as valor_buc_van,
            buc_brt_1a_perna as quantidade_transacao_buc_brt_perna_1,
            buc_brt_2a_perna as quantidade_transacao_buc_brt_perna_2,
            buc_brt_3a_perna as quantidade_transacao_buc_brt_perna_3,
            buc_brt_receita as valor_buc_brt,
            buc_inter_1a_perna as quantidade_transacao_buc_inter_perna_1,
            buc_inter_2a_perna as quantidade_transacao_buc_inter_perna_2,
            buc_inter_receita as valor_buc_inter,
            buc_metro_1a_perna as quantidade_transacao_buc_metro_perna_1,
            buc_metro_2a_perna as quantidade_transacao_buc_metro_perna_2,
            buc_metro_receita as valor_buc_metro,
            cartao as quantidade_transacao_cartao,
            receita_cartao as valor_cartao,
            especie_passageiro_transportado as quantidade_transacao_especie,
            especie_receita as valor_especie,
            data_processamento,
            timestamp_captura as datetime_captura
        from {{ staging_table }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    rdo_complete_partitions as (
        select *
        from rdo_new

        {% if is_incremental() and partition_list | length > 0 %}

            union all

            select * except (versao, datetime_ultima_atualizacao)
            from {{ this }}
            where data_transacao in ({{ partition_list | join(", ") }})

        {% endif %}
    ),
    aux_dedup as (
        select data_arquivo_rdo, max(datetime_captura) as max_datetime_captura
        from rdo_complete_partitions
        group by 1
    )
select
    r.*,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from rdo_complete_partitions r
join aux_dedup a using (data_arquivo_rdo)
where a.max_datetime_captura = r.datetime_captura
