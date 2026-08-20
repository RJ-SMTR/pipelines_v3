{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Oferta planejada por faixa — contrato IPA (viagens_programadas) + lote
  do POR (`lote_servico`). Entrada `planejamento` do openfisca_smtr.apurar.
  Fonte: planejamento.servico_planejado_faixa_horaria (partidas → viagens_programadas).
  WIP/teste: lote A0 na ausência de POR (`lote_padrao_teste` / var `lote_padrao`).
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    oferta as (
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
            feed_version
        from {{ ref("servico_planejado_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    com_lote as (
        select
            o.*,
            {{ lote_padrao_teste("ls.lote") }} as lote,
            row_number() over (
                partition by o.data, o.servico, o.sentido, o.faixa_horaria_inicio
                order by ls.data_inicio desc
            ) as rn_lote
        from oferta o
        left join
            {{ ref("lote_servico") }} as ls
            on ls.servico = o.servico
            and (ls.data_inicio is null or o.data >= ls.data_inicio)
            and (ls.data_fim is null or o.data <= ls.data_fim)
    )
select
    data,
    tipo_dia,
    servico,
    sentido,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    viagens_programadas,
    lote,
    extensao,
    quilometragem,
    consorcio,
    modo,
    feed_start_date,
    feed_version,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from com_lote
where rn_lote = 1
