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
  Recorte de faixa de `aux_viagem_apurada` — colunas de `granularity`
  "faixa" na planilha Tabelas Remuneração Sistema RIO (2026-09-14).
  Grão: data × lote × serviço × sentido × faixa.

  O OF repete os totais da faixa em cada viagem, então o `distinct` só
  desfaz esse broadcast. `id_apuracao` fica de fora de propósito: é chave
  de viagem e traria o grão de volta para viagem, inflando as somas dos
  sumários a jusante.
#}
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
