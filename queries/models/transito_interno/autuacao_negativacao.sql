{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_autuacao"],
        incremental_strategy="insert_overwrite",
    )
}}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct concat("'", date(data_autuacao), "'") as partition_date
            from {{ ref("aux_retorno_negativacao") }}
            where data = date('{{ var("date_range_end") }}')
        {% endset %}
        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    aux_retorno_negativacao as (
        select
            data,
            data_autuacao,
            datetime_retorno,
            produtonome,
            produtoreferencia,
            protocolo,
            id_autuacao,
            cpf,
            resultado
        from {{ ref("aux_retorno_negativacao") }}
        where data = date('{{ var("date_range_end") }}')
    ),
    aux_autuacao_negativacao as (
        select
            data,
            id_autuacao,
            data_inclusao,
            data_baixa,
            indicador_nao_inclusao,
            motivo,
            nome,
            cpf,
            cnpj,
            endereco,
            bairro,
            cidade,
            cep,
            estado,
            contrato,
            datavencimento,
            datavenda,
            valor,
            valor_pagamento
        from {{ ref("aux_autuacao_negativacao") }}
        where data in {{ partitions }}
    ),
    confirmacao_negativacao as (
        select
            data,
            id_autuacao,
            data_inclusao,
            data_baixa,
            case
                when resultado = '00 - INCLUSAO' then date(datetime_retorno)
            end as data_confirmacao_inclusao,
            case
                when resultado = '01 - BAIXA' then date(datetime_retorno)
            end as data_confirmacao_baixa,
            indicador_nao_inclusao,
            motivo,
            nome,
            cpf,
            cnpj,
            endereco,
            bairro,
            cidade,
            cep,
            estado,
            contrato,
            datavencimento,
            datavenda,
            valor,
            valor_pagamento
        from aux_autuacao_negativacao
        left join
            aux_retorno_negativacao
            on aux_autuacao_negativacao.id_autuacao
            = aux_retorno_negativacao.id_autuacao
            and aux_autuacao_negativacao.data = aux_retorno_negativacao.data_autuacao
    )
select
    *,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from confirmacao_negativacao
