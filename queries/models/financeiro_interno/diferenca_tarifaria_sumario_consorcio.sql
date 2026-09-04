{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    transacao_viagem as (
        select *
        from {{ ref("transacao_viagem_consorcio_planejamento") }}
        {# from `rj-smtr-dev.rodrigo__subsidio_staging.transacao_viagem_consorcio_planejamento` #}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    diferenca_tarifaria as (
        select *
        from {{ ref("diferenca_tarifaria_sumario_servico_faixa_sentido") }}
        {# from `rj-smtr-dev.rodrigo__financeiro_interno.diferenca_tarifaria_sumario_servico_faixa_sentido` #}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    transacao_viagem_consorcio_agg as (
        select
            data,
            consorcio,
            sum(
                if(
                    id_viagem is null or faixa_horaria_inicio is null,
                    valor_pagamento,
                    0
                )
            ) as receita_tarifa_publica_nao_associada
        from transacao_viagem
        group by 1, 2
    ),
    diferenca_tarifaria_cosorcio_agg as (
        select data, consorcio, sum(0) as delta_tr,
        from diferenca_tarifaria
        group by 1, 2
    )
select
    data,
    consorcio,
    {# receita_tarifa_publica_nao_associada, #}
    (receita_tarifa_publica_nao_associada/4.7)*5 as receita_tarifa_publica_nao_associada,
    {# delta_tr, #}
    {# delta_tr - receita_tarifa_publica_nao_associada as delta_tr_atualizado, #}
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from diferenca_tarifaria_cosorcio_agg
join transacao_viagem_consorcio_agg using (data, consorcio)
