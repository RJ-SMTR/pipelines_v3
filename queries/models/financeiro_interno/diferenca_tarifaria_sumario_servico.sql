{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    diferenca_tarifaria_sumario_servico as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            any_value(irk) as irk,
            any_value(subsidio_km) as subsidio_km,
            sum(viagens_faixa) as viagens_dia,
            sum(km_atendida_faixa) as km_atendida_dia,
            sum(km_conforme_faixa) as km_conforme_dia,
            sum(km_planejada_faixa) as km_planejada_dia,
            sum(receita_tarifa_publica_faixa) as receita_tarifa_publica_dia,
            safe_cast(
                coalesce(
                    round(
                        100 * sum(km_atendida_faixa)
                        / sum(km_planejada_faixa),
                        2
                    ),
                    0
                ) as numeric
            ) as percentual_atendimento_dia,
            sum(valor_penalidade_faixa) as valor_penalidade_dia,
        from
            {{ ref("diferenca_tarifaria_sumario_servico_faixa_sentido") }}
            {# `rj-smtr-dev`.`rodrigo__financeiro_interno`.`diferenca_tarifaria_sumario_servico_faixa_sentido` #}
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
        group by all
    )
select
    data,
    tipo_dia,
    consorcio,
    servico,
    irk,
    subsidio_km,
    viagens_dia,
    km_atendida_dia,
    km_conforme_dia,
    km_planejada_dia,
    percentual_atendimento_dia,
    receita_tarifa_publica_dia,
    valor_penalidade_dia,
    -- Cenário B1 - Resultado final ADT por dia ignorando abaixo de 80%
    if(
        percentual_atendimento_dia >= 80,
        (km_conforme_dia * subsidio_km) + (km_atendida_dia * (irk - subsidio_km)) - receita_tarifa_publica_dia,
        0
    )
    + valor_penalidade_dia as delta_tr_b1,
    -- Cenário B2 - Resultado final ADT por dia, incluindo os dias abaixo de 80%, comparando com a km conforme*IRK. Não paga quando for positivo.
    if(
        percentual_atendimento_dia >= 80,
        (km_conforme_dia * subsidio_km) + (km_atendida_dia * (irk - subsidio_km)) - receita_tarifa_publica_dia,
        (km_atendida_dia * (irk - subsidio_km)) - receita_tarifa_publica_dia
    )
    + valor_penalidade_dia as delta_tr_b2,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from diferenca_tarifaria_sumario_servico
