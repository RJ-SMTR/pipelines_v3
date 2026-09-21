{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{% if execute %}
    {% set gtfs_feeds_query %}
        select distinct concat("'", feed_start_date, "'") as feed_start_date
        from {{ calendario }}
        where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% set feed_filter %}
    {% if gtfs_feeds | length > 0 %} feed_start_date in ({{ gtfs_feeds | join(", ") }})
    {% else %} 1 = 0
    {% endif %}
    {% endset %}
{% endif %}

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
            quilometragem as km,
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
        where regexp_contains(agency_id, r"^[A-Z][0-9]$") and {{ feed_filter }}
    )
select
    o.data,
    o.tipo_dia,
    o.servico,
    o.sentido,
    o.faixa_horaria_inicio,
    o.faixa_horaria_fim,
    o.viagens_programadas,
    ls.lote,
    o.extensao,
    o.km,
    cast(0 as int64) as lote_frota_estimada,
    cast(0 as int64) as lote_frota_determinada,
    cast(null as float64) as lote_qr_mensal,
    cast(null as float64) as lote_km_referencia_mensal,
    cast(null as float64) as lote_km_referencia,
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
