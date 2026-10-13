{{
    config(
        materialized="table",
    )
}}

{% set var_table_exists = table_exists(this) %}

{% if var_table_exists %}
    {% set ultima_data_captura_query %}
        select
            concat(
                "'", max(if(conta = 'CETT', date(datetime_captura), null)), "'"
            ) as data_cett,
            concat(
                "'", max(if(conta = 'CB', date(datetime_captura), null)), "'"
            ) as data_cb,
            concat(
                "'", max(if(conta = 'CAER', date(datetime_captura), null)), "'"
            ) as data_caer,
        from {{ this }}
    {% endset %}

    {% set ultima_data_captura_result = run_query(ultima_data_captura_query) %}

    {% set ultima_data_captura_cett = ultima_data_captura_result.columns[0].values()[
        0
    ] %}
    {% set ultima_data_captura_cb = ultima_data_captura_result.columns[1].values()[0] %}
    {% set ultima_data_captura_caer = ultima_data_captura_result.columns[2].values()[
        0
    ] %}
{% endif %}

with
    cett_staging as (
        select
            data_operacao as data,
            tipo,
            operacao,
            favorecido,
            modal,
            valor,
            ano_competencia,
            mes_competencia,
            if(
                trim(quinzena_competencia) = '', null, quinzena_competencia
            ) as quinzena_semana_competencia,
            if(trim(numero_processo) = '', null, numero_processo) as numero_processo,
            if(trim(observacoes) = '', null, observacoes) as observacoes,
            cast(null as string) as numero_nf,
            'CETT' as tipo_conta,
            timestamp_captura as datetime_captura
        from {{ ref("staging_cett") }}
        {% if var_table_exists %}
            where data_captura >= {{ ultima_data_captura_cett }}
        {% endif %}
        qualify timestamp_captura = max(timestamp_captura) over ()
    ),
    cb_staging as (
        select
            data_operacao as data,
            tipo,
            operacao,
            favorecido,
            modal,
            valor,
            ano_competencia,
            mes_competencia,
            if(
                trim(quinzena_semana_competencia) = '',
                null,
                quinzena_semana_competencia
            ) as quinzena_semana_competencia,
            if(trim(numero_processo) = '', null, numero_processo) as numero_processo,
            if(trim(observacoes) = '', null, observacoes) as observacoes,
            if(trim(numero_nf) = '', null, numero_nf) as numero_nf,
            'CB' as tipo_conta,
            timestamp_captura as datetime_captura
        from {{ ref("staging_cb") }}
        {% if var_table_exists %}
            where data_captura >= {{ ultima_data_captura_cb }}
        {% endif %}
        qualify timestamp_captura = max(timestamp_captura) over ()
    ),
    caer_staging as (
        select
            data_operacao as data,
            tipo,
            operacao,
            favorecido,
            cast(null as string) as modal,
            valor,
            ano_competencia,
            mes_competencia,
            cast(null as string) as quinzena_semana_competencia,
            if(trim(numero_processo) = '', null, numero_processo) as numero_processo,
            if(trim(observacoes) = '', null, observacoes) as observacoes,
            if(trim(numero_nf) = '', null, numero_nf) as numero_nf,
            'CAER' as tipo_conta,
            timestamp_captura as datetime_captura
        from {{ ref("staging_caer") }}
        {% if var_table_exists %}
            where data_captura >= {{ ultima_data_captura_cb }}
        {% endif %}
        qualify timestamp_captura = max(timestamp_captura) over ()
    ),
    controle_cct_union as (
        select *
        from cett_staging

        union all

        select *
        from cb_staging

        union all

        select *
        from caer_staging
    ),
    controle_cct_colunas_controle as (
        select
            *,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from controle_cct_union
    )
select *
from controle_cct_colunas_controle
