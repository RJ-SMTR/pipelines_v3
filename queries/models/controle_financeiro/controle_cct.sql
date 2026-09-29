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
            data,
            lancamento,
            operacao,
            tipo,
            valor,
            saldo_final,
            favorecido,
            modal,
            'CETT' as conta,
            timestamp_captura as datetime_captura
        from {{ ref("staging_cett") }}
        {% if var_table_exists %}
            where data_captura >= {{ ultima_data_captura_cett }}
        {% endif %}
        qualify timestamp_captura = max(timestamp_captura) over ()
    ),
    cb_staging as (
        select
            data,
            lancamento,
            operacao,
            tipo,
            valor,
            saldo_final,
            favorecido,
            modal,
            'CB' as conta,
            timestamp_captura as datetime_captura
        from {{ ref("staging_cb") }}
        {% if var_table_exists %}
            where data_captura >= {{ ultima_data_captura_cb }}
        {% endif %}
        qualify timestamp_captura = max(timestamp_captura) over ()
    ),
    caer_staging as (
        select
            data,
            cast(null as string) as lancamento,
            operacao,
            tipo,
            valor,
            saldo_final,
            favorecido,
            cast(null as string) as modal,
            'CAER' as conta,
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
