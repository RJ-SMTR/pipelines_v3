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
{% endif %}

-- 1. Seleciona sinais de GPS registrados no período
with
    gps as (
        select
            g.* except (longitude, latitude, servico),
            servico,
            substr(id_veiculo, 2, 3) as id_empresa,
            st_geogpoint(longitude, latitude) as geo_point_gps,
            case
                when extract(hour from timestamp_gps) < 3
                then date_sub(extract(date from timestamp_gps), interval 1 day)
                else extract(date from timestamp_gps)
            end as data_operacao
        from {{ ref("gps_sppo") }} g
        {# from `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g #}
        where
            data between date('{{ var("date_range_start") }}') and date_add(
                date('{{ var("date_range_end") }}'), interval 1 day
            )
            and timestamp_gps
            between datetime_trunc(
                date('{{ var("date_range_start") }}'),
                day
            ) and datetime_add(
                datetime_trunc(
                    date_add(date('{{ var("date_range_end") }}'), interval 1 day), day
                ),
                interval 3 hour
            )
            and status != "Parado garagem"
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
            data, feed_version, feed_start_date, servico, sentido, extensao, trip_info
        from {{ ref("servico_planejado_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    servico_planejado_unnested as (
        select
            sp.data,
            sp.feed_version,
            sp.feed_start_date,
            sp.servico,
            sp.sentido,
            sp.extensao,
            trip.trip_id,
            trip.route_id,
            trip.shape_id,
        from servico_planejado sp, unnest(sp.trip_info) as trip
        where trip.shape_id is not null
        qualify
            row_number() over (
                partition by sp.data, trip.route_id, trip.shape_id
                order by trip.primeiro_horario
            )
            = 1
    ),
    servico_planejado_shapes as (
        select spu.*, s.shape, s.start_pt, s.end_pt
        from servico_planejado_unnested as spu
        left join shapes as s using (feed_version, feed_start_date, shape_id)
    ),
    -- 4. Classifica a posição do veículo em todos os shapes possíveis de
    -- serviços de uma mesma empresa
    status_viagem as (
        select
            data_operacao as data,
            g.id_veiculo,
            g.id_empresa,
            g.timestamp_gps,
            g.geo_point_gps,
            trim(g.servico, " ") as servico_gps,
            s.servico as servico_viagem,
            s.shape_id,
            s.sentido,
            s.trip_id,
            s.route_id,
            s.start_pt,
            s.end_pt,
            s.extensao as distancia_planejada,
            ifnull(g.distancia, 0) as distancia,
            s.feed_start_date,
            case
                when st_dwithin(g.geo_point_gps, start_pt, {{ var("buffer") }})
                then 'start'
                when st_dwithin(g.geo_point_gps, end_pt, {{ var("buffer") }})
                then 'end'
                when st_dwithin(g.geo_point_gps, shape, {{ var("buffer") }})
                then 'middle'
                else 'out'
            end status_viagem
        from gps g
        inner join
            servico_planejado_shapes s
            on g.data_operacao = s.data
            and g.servico = s.servico
    ),
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
    -- Adiciona o indicador de interseção com o primeiro e último segmento, meio
    -- sempre true
    status_viagem_segmentos as (
        select
            sv.*,
            case
                when
                    sv.status_viagem = 'start'
                    and st_intersects(sv.geo_point_gps, sf.buffer_inicio)
                then true
                when
                    sv.status_viagem = 'end'
                    and st_intersects(sv.geo_point_gps, sf.buffer_fim)
                then true
                else false
            end as indicador_intersecao_segmento,
        from status_viagem sv
        left join
            segmentos_filtrados sf
            on sv.shape_id = sf.shape_id
            and sv.feed_start_date = sf.feed_start_date
    )
select *
from status_viagem_segmentos
