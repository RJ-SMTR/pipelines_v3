{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    -- Transações Jaé
    transacao as (
        select
            data,
            id_transacao,
            id_validador,
            id_veiculo,
            servico_jae as servico,
            consorcio,
            valor_pagamento as valor_transacao,
            cast((valor_pagamento / 0.96) as numeric) as valor_transacao_rateio,
            datetime_transacao
        from {{ ref("transacao") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
            and not (
                length(ifnull(regexp_extract(servico_jae, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(servico_jae, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviários
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
    ),
    -- Transações RioCard
    transacao_riocard as (
        select
            data,
            id_transacao,
            id_validador,
            id_veiculo,
            servico_jae as servico,
            consorcio,
            valor_transacao,
            cast(
                if(data < '2025-08-02', null, 4.7) as numeric
            ) as valor_transacao_rateio,
            datetime_transacao
        -- from {{ ref("transacao_riocard") }}
        from `rj-smtr.bilhetagem.transacao_riocard`
        where
            data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
            and not (
                length(ifnull(regexp_extract(servico_jae, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(servico_jae, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviários
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
    ),
    transacoes as (
        select *
        from transacao
        union all by name
        select *
        from transacao_riocard
    ),
    viagem_transacao as (
        select
            data,
            servico,
            sentido,
            id_viagem,
            id_veiculo,
            datetime_partida_bilhetagem,
            datetime_partida,
            datetime_chegada
        from {{ ref("viagem_transacao") }}
        -- from `rj-smtr.subsidio.viagem_transacao`
        where
            data
            between date_sub(date('{{ var("start_date") }}'), interval 1 day) and date(
                '{{ var("end_date") }}'
            )
    ),
    viagem_planejada as (
        select
            data, servico, sentido, id_viagem, faixa_horaria_inicio, faixa_horaria_fim
        from {{ ref("viagens_remuneradas") }}
        where
            data
            between date_sub(date('{{ var("start_date") }}'), interval 1 day) and date(
                '{{ var("end_date") }}'
            )
    ),
    viagens as (
        select t.*, faixa_horaria_inicio, faixa_horaria_fim
        from viagem_transacao t
        left join viagem_planejada p using (data, servico, sentido, id_viagem)
    ),
    transacao_viagem_consorcio as (
        select
            coalesce(v.data, t.data) as data,
            id_transacao,
            id_validador,
            datetime_transacao,
            valor_transacao,
            valor_transacao_rateio as valor_pagamento,
            id_viagem,
            consorcio,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from transacoes t
        left join
            viagens v
            on t.id_veiculo = substr(v.id_veiculo, 2)
            and t.datetime_transacao
            between v.datetime_partida_bilhetagem and v.datetime_chegada
    )
select *
from transacao_viagem_consorcio
where data between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
