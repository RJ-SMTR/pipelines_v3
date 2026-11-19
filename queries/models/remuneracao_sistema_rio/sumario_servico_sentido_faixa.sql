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
    apurada as (
        select
            data,
            servico,
            sentido,
            faixa_horaria_inicio,
            any_value(viagens_atendimento_faixa) as viagens_atendimento_faixa,
            any_value(viagens_programadas_faixa) as viagens_programadas_faixa,
            any_value(percentual_atendimento) as percentual_atendimento,
            any_value(ipa) as ipa,
            any_value(desconto_operacao_precaria) as desconto_operacao_precaria,
            any_value(km_remuneravel_faixa) as km_remuneravel_faixa,
            any_value(km_ponderada_ipa_faixa) as km_ponderada_ipa_faixa_openfisca
        from {{ ref("aux_viagem_apurada") }}
        where {{ incremental_filter }}
        group by data, servico, sentido, faixa_horaria_inicio
    ),
    apuracao_lote_dia as (
        select data, lote, tarifa_remuneracao, beta
        from {{ ref("sumario_lote_dia") }}
        where {{ incremental_filter }}
    ),
    faixa_atendimento as (
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
            a.percentual_atendimento,
            a.desconto_operacao_precaria,
            coalesce(a.km_remuneravel_faixa, 0) as km_remuneravel_faixa,
            o.km_planejada,
            a.km_ponderada_ipa_faixa_openfisca,
            a.ipa,
            d.tarifa_remuneracao,
            d.beta
        from oferta as o
        left join
            apurada as a
            on a.data = o.data
            and a.servico = o.servico
            and a.sentido = o.sentido
            and a.faixa_horaria_inicio = o.faixa_horaria_inicio
        left join apuracao_lote_dia as d on d.data = o.data and d.lote = o.lote
    ),
    faixa_ipa as (
        select *, least(km_remuneravel_faixa, km_planejada) as km_opex_faixa
        from faixa_atendimento
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
    coalesce(
        km_ponderada_ipa_faixa_openfisca, km_opex_faixa * ipa
    ) as km_ponderada_ipa_faixa,
    tarifa_remuneracao
    * beta
    * coalesce(
        km_ponderada_ipa_faixa_openfisca, km_opex_faixa * ipa
    ) as remuneracao_opex_faixa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from faixa_ipa
