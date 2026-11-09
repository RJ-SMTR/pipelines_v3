{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    oferta as (
        select
            data,
            tipo_dia,
            lote,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            viagens_programadas as viagens_programadas_faixa,
            km as km_planejada
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }}
    ),
    km_faixa as (
        select
            data,
            servico,
            sentido,
            faixa_horaria_inicio,
            sum(if(indicador_viagem_completa, km_programada, 0)) as km_remuneravel_faixa,
            count(*) as viagens_realizadas_faixa
        from {{ ref("viagem_valida_classificada") }}
        where {{ incremental_filter }}
        group by data, servico, sentido, faixa_horaria_inicio
    ),
    apurada as (
        select
            data,
            servico,
            sentido,
            faixa_horaria_inicio,
            any_value(viagens_atendimento_faixa) as viagens_atendimento_faixa,
            any_value(desconto_operacao_precaria) as desconto_operacao_precaria
        from {{ ref("faixa_apurada") }}
        where {{ incremental_filter }}
        group by data, servico, sentido, faixa_horaria_inicio
    ),
    dia as (
        select data, lote, tarifa_remuneracao, beta
        from {{ ref("data_apurada") }}
        where {{ incremental_filter }}
    ),
    medida as (
        select
            o.data,
            o.tipo_dia,
            o.lote,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            o.consorcio,
            o.servico,
            o.sentido,
            a.viagens_atendimento_faixa,
            o.viagens_programadas_faixa,
            safe_divide(
                coalesce(k.viagens_realizadas_faixa, 0), o.viagens_programadas_faixa
            ) as percentual_atendimento,
            coalesce(k.viagens_realizadas_faixa, 0) as viagens_realizadas_faixa,
            a.desconto_operacao_precaria,
            coalesce(k.km_remuneravel_faixa, 0) as km_remuneravel_faixa,
            o.km_planejada,
            d.tarifa_remuneracao,
            d.beta
        from oferta as o
        left join
            km_faixa as k
            on k.data = o.data
            and k.servico = o.servico
            and k.sentido = o.sentido
            and k.faixa_horaria_inicio = o.faixa_horaria_inicio
        left join
            apurada as a
            on a.data = o.data
            and a.servico = o.servico
            and a.sentido = o.sentido
            and a.faixa_horaria_inicio = o.faixa_horaria_inicio
        left join dia as d on d.data = o.data and d.lote = o.lote
    ),
    com_ipa as (
        select
            *,
            least(
                km_remuneravel_faixa, km_planejada)
            as km_opex_faixa,
            case
                when
                    viagens_programadas_faixa > 0
                    and viagens_realizadas_faixa * 100 >= viagens_programadas_faixa * 90
                then 1.0
                when
                    viagens_programadas_faixa > 0
                    and viagens_realizadas_faixa * 100 >= viagens_programadas_faixa * 80
                then 0.9
                when
                    viagens_programadas_faixa > 0
                    and viagens_realizadas_faixa * 100 >= viagens_programadas_faixa * 60
                then 0.6
                else 0.0
            end as ipa
        from medida
    )
select
    data,
    tipo_dia,
    lote,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    consorcio,
    servico,
    sentido,
    viagens_atendimento_faixa,
    viagens_programadas_faixa,
    percentual_atendimento,
    ipa,
    desconto_operacao_precaria,
    km_remuneravel_faixa,
    km_opex_faixa * com_ipa.ipa as km_ponderada_ipa_faixa,
    tarifa_remuneracao * beta * km_opex_faixa * com_ipa.ipa as remuneracao_opex_faixa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from com_ipa
