{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Oferta planejada por faixa — contrato IPA (viagens_programadas) + lote.
  Entrada `planejamento` do `rio_rac_bus_subsidy.process_trip_calculations`.
  Fonte: planejamento.servico_planejado_faixa_horaria (partidas → viagens_programadas).
  Lote = agency_id GTFS do Sistema RIO (`A0`/`A2`/`B1`/`B2`).
  WIP: `lote_padrao_teste` só se o serviço não tiver agency_id de lote.
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
            feed_start_date
        from {{ ref("servico_planejado_faixa_horaria") }}
        where {{ incremental_filter }} and sistema = "RIO"
    ),
    lote_servico as (
        select
            feed_start_date,
            feed_version,
            route_short_name as servico,
            agency_id as lote,
            row_number() over (
                partition by feed_start_date, route_short_name order by agency_id
            ) as rn
        from {{ ref("routes_gtfs") }}
        where regexp_contains(agency_id, r"^[A-Z][0-9]$")
    )
select
    o.data,
    o.tipo_dia,
    o.servico,
    o.sentido,
    o.faixa_horaria_inicio,
    o.faixa_horaria_fim,
    o.viagens_programadas,
    {{ lote_padrao_teste("ls.lote") }} as lote,
    o.extensao,
    o.quilometragem,
    o.consorcio,
    o.modo,
    o.feed_start_date,
    ls.feed_version,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from oferta as o
left join
    lote_servico as ls
    on ls.feed_start_date = o.feed_start_date
    and ls.servico = o.servico
    and ls.rn = 1
