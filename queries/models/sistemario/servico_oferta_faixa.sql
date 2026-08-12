{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Oferta planejada por faixa — contrato IPA (viagens_programadas).
  Fonte: planejamento.servico_planejado_faixa_horaria (partidas → viagens_programadas).
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

select
    data,
    tipo_dia,
    servico,
    sentido,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    safe_cast(partidas as int64) as viagens_programadas,
    extensao,
    quilometragem,
    consorcio,
    modo,
    feed_start_date,
    feed_version,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("servico_planejado_faixa_horaria") }}
where {{ incremental_filter }}
