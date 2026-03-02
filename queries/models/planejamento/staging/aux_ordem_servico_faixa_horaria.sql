{{ config(materialized="view") }}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute %}
    {% set gtfs_feeds_query %}
        select distinct concat("'", feed_start_date, "'") as feed_start_date
        from {{ calendario }}
        where
            data between date("{{ var('date_range_start') }}")
            and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{% endif %}

with
    os as (
        select * from {{ ref("aux_ordem_servico_diaria") }}
    {# from `rj-smtr.gtfs.ordem_servico` #}
    ),
    faixa as (
        select * from {{ ref("ordem_servico_faixa_horaria") }}
    {# from `rj-smtr.planejamento.ordem_servico_faixa_horaria` #}
    )
select
    f.feed_version,
    f.feed_start_date,
    f.tipo_os,
    f.servico,
    o.vista,
    f.consorcio,
    o.extensao_ida,
    o.extensao_volta,
    f.tipo_dia,
    o.horario_inicio,
    o.horario_fim,
    o.partidas_ida as partidas_ida_dia,
    o.partidas_volta as partidas_volta_dia,
    o.viagens_planejadas as viagens_dia,
    f.faixa_horaria_inicio,
    f.faixa_horaria_fim,
    safe_cast(null as int64) as partidas_ida,
    safe_cast(null as int64) as partidas_volta,
    f.partidas,
    f.quilometragem,
    f.versao_modelo
from faixa as f
left join os as o using (feed_start_date, tipo_os, tipo_dia, servico)
where feed_start_date in ({{ gtfs_feeds | join(", ") }})
