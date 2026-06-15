{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    percentual_operacao as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            km_planejada_faixa,
            consorcio,
            servico,
            sentido,
            pof
        -- from {{ ref("percentual_operacao_faixa_horaria") }}
        from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    viagens as (
        select
            data,
            datetime_partida,
            servico,
            sentido,
            id_viagem,
            safe_cast(distancia_planejada as numeric) as distancia_planejada,
            receita_tarifa_publica,
            irk,
            subsidio_km,
            indicador_viagem_dentro_limite,
            indicador_conformidade,
            indicador_validade
        from {{ ref("viagens_remuneradas") }}
        {# from `rj-smtr-dev`.`victor__dashboard_subsidio_sppo`.`viagens_remuneradas` #}
        -- `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    penalidade as (
        select
            data,
            tipo_dia,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            {# valor_penalidade, #}
            valor_penalidade*1.144812527 as valor_penalidade -- [Teórico considerando hipotético out/25 em 2026]
        from {{ ref("subsidio_penalidade_servico_faixa") }}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    subsidio_servico as (
        select
            p.data,
            p.tipo_dia,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            p.consorcio,
            p.servico,
            p.sentido,
            p.pof,
            {# any_value(irk) over (partition by p.data) as irk,
            any_value(subsidio_km) over (partition by p.data) as subsidio_km, #}
            9 as irk, -- [Teórico considerando hipotético out/25 em 2026]
            3.06 as subsidio_km, -- [Teórico considerando hipotético out/25 em 2026]
            v.id_viagem,
            receita_tarifa_publica,
            p.km_planejada_faixa,
            distancia_planejada,
            v.indicador_viagem_dentro_limite,
            indicador_conformidade,
            indicador_validade
        from percentual_operacao as p
        left join
            viagens as v
            on p.data = v.data
            and p.servico = v.servico
            and p.sentido = v.sentido
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    ),
    subsidio_km as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            sentido,
            pof,
            irk,
            subsidio_km,
            count(*) as viagens_faixa,
            any_value(km_planejada_faixa) as km_planejada_faixa,
            sum(
                if(
                    indicador_conformidade and indicador_viagem_dentro_limite,
                    distancia_planejada,
                    0
                )
            ) as km_conforme_faixa,
            sum(if(indicador_validade, distancia_planejada, 0)) as km_atendida_faixa,
            {# coalesce(sum(receita_tarifa_publica), 0) as receita_tarifa_publica_faixa, #}
            coalesce(sum((receita_tarifa_publica/4.7)*5), 0) as receita_tarifa_publica_faixa, -- [receita/tarifa = passageiro_equivalente - Teórico considerando hipotético out/25 em 2026]
        from subsidio_servico
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )
select
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    viagens_faixa,
    pof as percentual_atendimento_faixa,
    irk,
    subsidio_km,
    km_planejada_faixa,
    km_conforme_faixa,
    km_atendida_faixa,
    receita_tarifa_publica_faixa,
    km_conforme_faixa * irk as receita_irk_faixa,
    valor_penalidade as valor_penalidade_faixa,
    -- Cenário C1 - Resultado final ADT por faixa horária, ignorando abaixo de 80%
    if(
        pof >= 80,
        (km_conforme_faixa * s.subsidio_km) + (km_atendida_faixa * (irk - s.subsidio_km)) - receita_tarifa_publica_faixa,
        0
    )
    + valor_penalidade as delta_tr_c1,
    
    -- Cenário C2 - Resultado final ADT por faixa horária, incluindo os dias abaixo de 80%, comparando com a km conforme*IRK. Não paga quando for positivo.
    
    if(
        pof >= 80,
        (km_conforme_faixa * s.subsidio_km) + (km_atendida_faixa * (irk - s.subsidio_km)) - receita_tarifa_publica_faixa,
        (km_atendida_faixa * (irk - s.subsidio_km)) - receita_tarifa_publica_faixa
    )
    + valor_penalidade as delta_tr_c2,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from subsidio_km s
left join
    penalidade using (
        data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, servico, sentido
    )
