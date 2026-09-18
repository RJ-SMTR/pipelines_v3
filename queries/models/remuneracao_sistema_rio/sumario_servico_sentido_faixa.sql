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
  Sumário por serviço × sentido × faixa (planilha Tabelas Remuneração
  Sistema RIO, 2026-09-14). Base: `faixa_apurada`.

  `consorcio` não existe na cadeia de apuração — vem de
  `servico_oferta_faixa`, a mesma fonte que define o lote, para lote e
  consórcio não divergirem.

  OPEX da faixa = tarifa × β × km ponderada pelo IPA. Como
  `km_ponderada_ipa_faixa` já é `km_remuneravel_faixa * ipa`, a fórmula da
  planilha [tarifa_remuneracao * beta * km_remuneravel * ipa] reduz a isso.
  Tarifa e β são do dia, então vêm de `data_apurada`.
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    faixa as (select * from {{ ref("faixa_apurada") }} where {{ incremental_filter }}),
    dia as (
        select data, lote, tarifa_remuneracao, beta
        from {{ ref("data_apurada") }}
        where {{ incremental_filter }}
    ),
    consorcio_servico as (
        select distinct data, servico, consorcio
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }} and consorcio is not null
    )
select
    f.data,
    f.tipo_dia,
    f.lote,
    f.faixa_horaria_inicio,
    f.faixa_horaria_fim,
    c.consorcio,
    f.servico,
    f.sentido,
    f.viagens_atendimento_faixa,
    f.viagens_programadas_faixa,
    f.percentual_atendimento,
    f.ipa,
    f.desconto_operacao_precaria,
    f.km_remuneravel_faixa,
    f.km_ponderada_ipa_faixa,
    d.tarifa_remuneracao * d.beta * f.km_ponderada_ipa_faixa as remuneracao_opex_faixa,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from faixa as f
left join dia as d on d.data = f.data and d.lote = f.lote
left join consorcio_servico as c on c.data = f.data and c.servico = f.servico
