{{
    config(
        materialized="ephemeral",
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
    {% if gtfs_feeds | length == 0 %}
        {% set gtfs_feeds = ["'2000-01-01'"] %}
    {% endif %}
{% endif %}

-- 1. Seleciona sinais de GPS registrados no período
with
    gps as (
        select
            g.* except (longitude, latitude),
            st_geogpoint(g.longitude, g.latitude) as geo_point_gps,
            case
                when extract(hour from datetime_gps) < 3
                then date_sub(extract(date from datetime_gps), interval 1 day)
                else extract(date from datetime_gps)
            end as data_operacao
        from {{ ref("aux_gps_viagem_inferida") }} g
    ),
    -- 2. Busca os shapes em formato geográfico
    shapes as (
        select *
        from {{ ref("shapes_geom_gtfs") }}
        {# from `rj-smtr.gtfs.shapes_geom` #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    servico_planejado as (
        select
            data,
            feed_start_date,
            servico,
            consorcio,
            sistema,
            modo,
            sentido,
            extensao,
            faixa_horaria_inicio,
            trip_info,
            trajetos_alternativos
        from {{ ref("servico_planejado_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    servico_planejado_expandido as (
        select
            sp.data,
            sp.feed_start_date,
            sp.servico,
            sp.consorcio,
            sp.sistema,
            sp.modo,
            sp.sentido,
            sp.extensao,
            trip.trip_id,
            trip.route_id,
            trip.shape_id,
            trip.primeiro_horario as horario_ordenacao
        from servico_planejado as sp, unnest(sp.trip_info) as trip
        where trip.shape_id is not null

        union all

        select
            sp.data,
            sp.feed_start_date,
            sp.servico,
            sp.consorcio,
            sp.sistema,
            sp.modo,
            sp.sentido,
            alt.extensao,
            alt.trip_id,
            sp.trip_info[safe_offset(0)].route_id,
            alt.shape_id,
            sp.faixa_horaria_inicio as horario_ordenacao
        from servico_planejado as sp, unnest(sp.trajetos_alternativos) as alt
        where alt.shape_id is not null
    ),
    servico_planejado_unnested as (
        select
            data,
            feed_start_date,
            servico,
            consorcio,
            sistema,
            modo,
            sentido,
            extensao,
            trip_id,
            route_id,
            shape_id,
        from servico_planejado_expandido
        qualify
            row_number() over (
                partition by data, route_id, shape_id order by horario_ordenacao
            )
            = 1
    ),
    servico_planejado_shapes as (
        select spu.*, s.shape, s.start_pt, s.end_pt
        from servico_planejado_unnested as spu
        left join shapes as s using (feed_start_date, shape_id)
    ),
    -- 4. Primeiro e último segmento de cada shape, antes de classificar o status
    segmentos_filtrados as (
        select
            shape_id,
            feed_start_date,
            array_agg(buffer order by safe_cast(id_segmento as int64) asc limit 1)[
                offset(0)
            ] as buffer_inicio,
            array_agg(buffer order by safe_cast(id_segmento as int64) desc limit 1)[
                offset(0)
            ] as buffer_fim
        from {{ ref("segmento_shape") }}
        {# from `rj-smtr.planejamento.segmento_shape` #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        group by shape_id, feed_start_date
    ),
    -- 5. Posição do GPS no primeiro segmento, no último e no shape
    posicao_segmento as (
        select
            data_operacao as data,
            g.id_veiculo,
            g.datetime_gps,
            g.fonte_gps,
            g.geo_point_gps,
            trim(g.servico, " ") as servico_gps,
            s.servico as servico_viagem,
            s.consorcio,
            s.sistema,
            s.modo,
            s.shape_id,
            s.sentido,
            s.trip_id,
            s.route_id,
            s.start_pt,
            s.end_pt,
            s.extensao as distancia_planejada,
            ifnull(g.distancia, 0) as distancia,
            s.feed_start_date,
            (
                s.sentido = "C"
                or {{
                    is_shape_circular(
                        "start_pt", "end_pt", "s.feed_start_date", "s.shape_id"
                    )
                }}
            ) as indicador_circular,
            ifnull(
                st_intersects(g.geo_point_gps, sf.buffer_inicio), false
            ) as indicador_segmento_inicio,
            ifnull(
                st_intersects(g.geo_point_gps, sf.buffer_fim), false
            ) as indicador_segmento_fim,
            st_dwithin(
                g.geo_point_gps, s.shape, {{ var("buffer") }}
            ) as indicador_no_shape
        from gps g
        inner join
            servico_planejado_shapes s
            on g.data_operacao = s.data
            and g.servico = s.servico
        left join
            segmentos_filtrados sf
            on s.shape_id = sf.shape_id
            and s.feed_start_date = sf.feed_start_date
    ),
    -- 6. Status a partir do primeiro e último segmento.
    -- Circular: fim do shape é end; o restante do shape é middle.
    -- Linear: primeiro segmento é start e último é end.
    status_base as (
        select
            * except (
                indicador_segmento_inicio, indicador_segmento_fim, indicador_no_shape
            ),
            case
                when indicador_circular and indicador_segmento_fim
                then "end"
                when
                    indicador_circular
                    and (indicador_no_shape or indicador_segmento_inicio)
                then "middle"
                when not indicador_circular and indicador_segmento_inicio
                then "start"
                when not indicador_circular and indicador_segmento_fim
                then "end"
                when indicador_no_shape
                then "middle"
                else "out"
            end as status_viagem
        from posicao_segmento
    ),
    -- 7. Circular: primeira comunicação no shape (middle_start) e
    -- primeira no end vinda do shape (middle_end).
    transicao_circular as (
        select
            * except (status_viagem),
            case
                when
                    indicador_circular
                    and status_viagem = "middle"
                    and ifnull(
                        lag(status_viagem) over (
                            partition by id_veiculo, shape_id
                            order by datetime_gps, fonte_gps
                        ),
                        ""
                    )
                    != "middle"
                then "middle_start"
                when
                    indicador_circular
                    and status_viagem = "end"
                    and lag(status_viagem) over (
                        partition by id_veiculo, shape_id
                        order by datetime_gps, fonte_gps
                    )
                    = "middle"
                then "middle_end"
                else status_viagem
            end as status_viagem
        from status_base
    ),
    status_viagem as (
        select
            * except (indicador_circular),
            case
                when status_viagem in ("middle_start", "middle_end")
                then true
                when not indicador_circular and status_viagem in ("start", "end")
                then true
                else false
            end as indicador_intersecao_segmento
        from transicao_circular
    )
select *
from status_viagem
