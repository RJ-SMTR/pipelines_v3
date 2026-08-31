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
            trajetos_alternativos
        from {{ ref("viagem_planejada_planejamento_dia") }}
        {# from `rj-smtr.planejamento.viagem_planejada` #}
        where {{ incremental_filter }}
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
            o.partidas,
            o.quilometragem,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
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
