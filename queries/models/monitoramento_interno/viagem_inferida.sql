{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_viagem"],
        incremental_strategy="insert_overwrite",
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
    aux_status as (
        select
            * except (servico_viagem),
            servico_viagem as servico,
            case
                when status_viagem = 'end'
                then
                    last_value(
                        case when status_viagem = 'start' then timestamp_gps end
                    ) over (
                        partition by id_veiculo, shape_id
                        order by timestamp_gps
                        rows between unbounded preceding and 1 preceding
                    )
            end as datetime_partida
        from {{ ref("aux_monitoramento_registros_status_trajeto") }}
        where indicador_intersecao_segmento = true
    ),
    routes as (
        select *
        from {{ ref("routes_gtfs") }}
        {# from `rj-smtr.gtfs.routes` #}
        where feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    viagens as (
        select
            concat(
                id_veiculo,
                "-",
                servico,
                "-",
                sentido,
                "-",
                shape_id,
                "-",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem,
            data,
            id_empresa,
            id_veiculo,
            servico_gps,
            servico,
            case
                when r.route_type = '200'
                then 'Ônibus Executivo'
                when r.route_type = '700'
                then 'Ônibus SPPO'
            end as modo,
            trip_id,
            route_id,
            shape_id,
            sentido,
            distancia_planejada,
            datetime_partida,
            timestamp_gps as datetime_chegada,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from aux_status
        left join routes r using (route_id, feed_start_date)
        where
            status_viagem = 'end'
            and datetime_partida is not null
            and datetime_partida < timestamp_gps
        qualify
            row_number() over (
                partition by id_veiculo, shape_id, datetime_partida
                order by timestamp_gps
            )
            = 1
    )
select *
from viagens v
