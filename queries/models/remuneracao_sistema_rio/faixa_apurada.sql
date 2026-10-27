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

select distinct
    lote,
    servico,
    sentido,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    tipo_dia,
    data,
    viagens_atendimento_faixa,
    viagens_programadas_faixa,
    percentual_atendimento,
    ipa,
    desconto_operacao_precaria,
    km_remuneravel_faixa,
    km_ponderada_ipa_faixa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("aux_viagem_apurada") }}
where {{ incremental_filter }}
