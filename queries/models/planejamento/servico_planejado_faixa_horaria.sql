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
{% endif %}

with
    os_sppo as (
        select *
        from {{ ref("aux_os_sppo_faixa_horaria_sentido_dia") }}
        where
            {{ incremental_filter }}
            and feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    viagens_planejadas as (
        select
            data,
            modo,
            servico,
            sentido,
            trip_id,
            route_id,
            shape_id,
            evento,
            datetime_partida,
            trajetos_alternativos,
            case
                when evento is not null then true else false
            end as indicador_trajeto_alternativo
        from {{ ref("viagem_planejada_planejamento") }}
        {# from `rj-smtr.planejamento.viagem_planejada` #}
        where
            {{ incremental_filter }}
            and feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    viagens_na_faixa as (
        select
            o.data,
            o.feed_version,
            o.feed_start_date,
            o.tipo_dia,
            o.tipo_os,
            o.servico,
            o.consorcio,
            o.sentido,
            o.extensao,
            o.partidas,
            o.quilometragem,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            v.modo,
            v.trip_id,
            v.route_id,
            v.shape_id,
            v.evento,
            v.indicador_trajeto_alternativo,
            v.trajetos_alternativos,
            min(datetime_partida) over (
                partition by
                    o.data, o.servico, o.sentido, o.faixa_horaria_inicio, v.trip_id
            ) as primeiro_horario,
            max(datetime_partida) over (
                partition by
                    o.data, o.servico, o.sentido, o.faixa_horaria_inicio, v.trip_id
            ) as ultimo_horario
        from os_sppo o
        left join
            viagens_planejadas v
            on o.data = v.data
            and o.servico = v.servico
            and o.sentido = v.sentido
            and v.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
    ),
    deduplicado as (
        select
            data,
            feed_version,
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
            trip_id,
            route_id,
            shape_id,
            evento,
            indicador_trajeto_alternativo,
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
                    shape_id,
                    evento,
                    indicador_trajeto_alternativo
            )
            = 1
    ),
    viagens_agrupadas as (
        select
            data,
            feed_version,
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
            array_agg(
                struct(
                    primeiro_horario as primeiro_horario,
                    ultimo_horario as ultimo_horario,
                    trip_id as trip_id,
                    route_id as route_id,
                    shape_id as shape_id,
                    evento as evento,
                    indicador_trajeto_alternativo as indicador_trajeto_alternativo
                )
            ) as trip_info,
            trajetos_alternativos
        from deduplicado
        group by
            data,
            feed_version,
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
            trajetos_alternativos
    )
select
    data,
    feed_version,
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
    trip_info,
    trajetos_alternativos,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagens_agrupadas
