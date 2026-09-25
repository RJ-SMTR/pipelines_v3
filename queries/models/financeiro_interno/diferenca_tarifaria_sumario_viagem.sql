{{ config(materialized="view") }}

with
    percentual_operacao as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
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
            indicador_viagem_dentro_limite,
            indicador_conformidade,
            indicador_validade
        from {{ ref("viagens_remuneradas") }}
        {# from `rj-smtr-dev`.`victor__dashboard_subsidio_sppo`.`viagens_remuneradas` #}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    viagem_adt as (
        select
            p.data,
            p.tipo_dia,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            p.consorcio,
            p.servico,
            p.sentido,
            v.id_viagem,
            v.datetime_partida,
            p.pof,
            9 as irk,  -- [Teórico considerando hipotético out/25 em 2026]
            3.06 as subsidio_km,  -- [Teórico considerando hipotético out/25 em 2026]
            v.distancia_planejada,
            v.indicador_conformidade,
            v.indicador_viagem_dentro_limite,
            v.indicador_validade,
            if(
                v.indicador_conformidade and v.indicador_viagem_dentro_limite,
                v.distancia_planejada,
                0
            ) as km_conforme_viagem,
            if(v.indicador_validade, v.distancia_planejada, 0) as km_atendida_viagem,
            coalesce(
                (v.receita_tarifa_publica / 4.7) * 5, 0
            ) as receita_tarifa_publica_viagem  -- [receita/tarifa = passageiro_equivalente - Teórico considerando hipotético out/25 em 2026]
        from percentual_operacao as p
        inner join
            viagens as v
            on p.data = v.data
            and p.servico = v.servico
            and p.sentido = v.sentido
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    )
select
    data,
    tipo_dia,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    id_viagem,
    datetime_partida,
    pof as percentual_atendimento_faixa,
    pof >= 80 as indicador_elegivel_adt,
    irk,
    subsidio_km,
    distancia_planejada,
    indicador_conformidade,
    indicador_viagem_dentro_limite,
    indicador_validade,
    km_conforme_viagem,
    km_atendida_viagem,
    receita_tarifa_publica_viagem,
    -- Cenário C3 por viagem: mesma fórmula híbrida da faixa, corte de 80% no POF
    if(
        pof >= 80,
        (km_conforme_viagem * subsidio_km)
        + (km_atendida_viagem * (irk - subsidio_km))
        - receita_tarifa_publica_viagem,
        0
    ) as delta_tr_c3,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_adt
