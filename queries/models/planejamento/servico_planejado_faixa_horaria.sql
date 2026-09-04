{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set intervalos = [
    {"inicio": 0, "fim": 1},
    {"inicio": 1, "fim": 2},
    {"inicio": 2, "fim": 3},
    {"inicio": 3, "fim": 4},
    {"inicio": 4, "fim": 5},
    {"inicio": 5, "fim": 6},
    {"inicio": 6, "fim": 9},
    {"inicio": 9, "fim": 12},
    {"inicio": 12, "fim": 15},
    {"inicio": 15, "fim": 18},
    {"inicio": 18, "fim": 21},
    {"inicio": 21, "fim": 22},
    {"inicio": 22, "fim": 23},
    {"inicio": 23, "fim": 24},
] %}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute %}
    {% set gtfs_feeds_query %}
            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario }}
            where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% set feed_filter %}
    {% if gtfs_feeds | length > 0 %}
        feed_start_date in ({{ gtfs_feeds | join(", ") }})
    {% else %} 1 = 0
    {% endif %}
    {% endset %}
{% endif %}

with
    os_sppo as (
        select *
        from {{ ref("aux_os_sppo_faixa_horaria_sentido_dia") }}
        where {{ incremental_filter }} and {{ feed_filter }}
    ),
    faixas_horarias_gtfs as (
        select *
        from
            unnest(
                [
                    {% for intervalo in intervalos %}
                        struct(
                            {{ intervalo.inicio }} as hora_inicio,
                            {{ intervalo.fim }} as hora_fim
                        )
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            )
    ),
    viagens_planejadas_com_faixa as (
        select
            v.*,
            datetime(v.data)
            + make_interval(hour => f.hora_inicio) as faixa_horaria_inicio_gtfs,
            datetime(v.data)
            + make_interval(hour => f.hora_fim)
            - interval 1 second as faixa_horaria_fim_gtfs
        from {{ ref("viagem_planejada_planejamento_dia") }} v
        left join
            faixas_horarias_gtfs f
            on v.datetime_partida
            >= datetime(v.data) + make_interval(hour => f.hora_inicio)
            and v.datetime_partida
            < datetime(v.data) + make_interval(hour => f.hora_fim)
        {# from `rj-smtr.planejamento.viagem_planejada` #}
        where {{ incremental_filter }}
    ),
    viagens_planejadas as (
        select
            data,
            feed_start_date,
            tipo_dia,
            tipo_os,
            modo,
            consorcio,
            sistema,
            vista,
            servico,
            sentido,
            trip_id,
            route_id,
            shape_id,
            datetime_partida,
            extensao,
            trajetos_alternativos,
            faixa_horaria_inicio_gtfs,
            faixa_horaria_fim_gtfs,
            count(*) over (win) as partidas_gtfs,
            sum(extensao) over (win) as quilometragem_gtfs
        from viagens_planejadas_com_faixa
        window
            win as (
                partition by
                    data,
                    feed_start_date,
                    tipo_dia,
                    tipo_os,
                    servico,
                    sentido,
                    faixa_horaria_inicio_gtfs,
                    faixa_horaria_fim_gtfs
            )
    ),
    viagens_na_faixa as (
        select
            v.data,
            v.feed_start_date,
            v.tipo_dia,
            v.tipo_os,
            v.servico,
            v.consorcio,
            v.sentido,
            v.extensao,
            coalesce(o.partidas, v.partidas_gtfs) as partidas,
            coalesce(o.quilometragem, v.quilometragem_gtfs) as quilometragem,
            coalesce(
                o.faixa_horaria_inicio, v.faixa_horaria_inicio_gtfs
            ) as faixa_horaria_inicio,
            coalesce(
                o.faixa_horaria_fim, v.faixa_horaria_fim_gtfs
            ) as faixa_horaria_fim,
            v.modo,
            v.sistema,
            v.vista,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.trajetos_alternativos,
            min(datetime_partida) over (
                partition by
                    v.data, v.servico, v.sentido, o.faixa_horaria_inicio, v.trip_id
            ) as primeiro_horario,
            max(datetime_partida) over (
                partition by
                    v.data, v.servico, v.sentido, o.faixa_horaria_inicio, v.trip_id
            ) as ultimo_horario
        from viagens_planejadas v
        left join
            os_sppo o
            on v.data = o.data
            and v.feed_start_date = o.feed_start_date
            and v.servico = o.servico
            and v.sentido = o.sentido
            and v.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
    ),
    deduplicado as (
        select
            data,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            modo,
            sistema,
            vista,
            trip_id,
            route_id,
            shape_id,
            trajetos_alternativos,
            primeiro_horario,
            ultimo_horario,
        from viagens_na_faixa
        qualify
            row_number() over (
                partition by
                    data,
                    servico,
                    sentido,
                    faixa_horaria_inicio,
                    trip_id,
                    route_id,
                    shape_id
            )
            = 1
    ),
    viagens_agrupadas as (
        select
            data,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            modo,
            sistema,
            vista,
            array_agg(
                struct(
                    primeiro_horario as primeiro_horario,
                    ultimo_horario as ultimo_horario,
                    trip_id as trip_id,
                    route_id as route_id,
                    shape_id as shape_id
                )
            ) as trip_info,
            trajetos_alternativos
        from deduplicado
        group by
            data,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            modo,
            sistema,
            vista,
            trajetos_alternativos
    )
select
    data,
    feed_start_date,
    tipo_dia,
    tipo_os,
    servico,
    consorcio,
    sistema,
    vista,
    sentido,
    extensao,
    partidas,
    quilometragem,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    modo,
    trip_info,
    trajetos_alternativos,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagens_agrupadas
