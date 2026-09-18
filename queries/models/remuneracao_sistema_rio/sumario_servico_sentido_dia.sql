{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Sumário por serviço × sentido × dia (planilha Tabelas Remuneração
  Sistema RIO, 2026-09-14). Soma as faixas de
  `sumario_servico_sentido_faixa`.
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

select
    data,
    tipo_dia,
    lote,
    consorcio,
    servico,
    sentido,
    sum(desconto_operacao_precaria) as desconto_operacao_precaria_dia,
    sum(remuneracao_opex_faixa) as remuneracao_opex_dia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("sumario_servico_sentido_faixa") }}
where {{ incremental_filter }}
group by data, tipo_dia, lote, consorcio, servico, sentido
