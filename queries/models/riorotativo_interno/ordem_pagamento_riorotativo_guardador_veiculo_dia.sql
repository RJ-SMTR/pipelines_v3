{{
    config(
        materialized="incremental",
        alias="ordem_pagamento_riorotativo_guardador_veiculo_dia",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

select
    data as data_ordem,
    cpf_guardador_veiculo,
    count(*) as quantidade_verificacao_total,
    countif(indicador_verificacao_valida) as quantidade_verificacao_valida,
    countif(not indicador_verificacao_valida) as quantidade_verificacao_invalida,
    sum(valor_repasse_guardador_veiculo) as valor_repasse_guardador_veiculo,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("verificacao_guardador_veiculo") }}
where
    data between date("{{ var('date_range_start') }}") and date(
        "{{ var('date_range_end') }}"
    )
group by data_ordem, cpf_guardador_veiculo
