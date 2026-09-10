{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="viagem_informada",
    )
}}


{% set incremental_filter %}
    date(data) between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% set staging_viagem_informada_rioonibus = ref("staging_viagem_informada_rioonibus") %}
{# {% set staging_viagem_informada_rioonibus = ("rj-smtr.monitoramento_staging.viagem_informada_rioonibus") %} #}
{% set staging_viagem_informada_brt = ref("staging_viagem_informada_brt") %}
{# {% set staging_viagem_informada_brt = ("rj-smtr.monitoramento_staging.viagem_informada_brt") %} #}
{% set staging_viagem_informada_maxtrack = ref("staging_viagem_informada_maxtrack") %}
{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct
                concat(
                    "'",
                    coalesce(
                        extract(date from datetime_partida),
                        extract(
                            date
                            from
                                datetime(
                                    timestamp(
                                        safe.parse_datetime(
                                            '%Y%m%d%H%M%S',
                                            split(id_viagem, '_')[
                                                safe_offset(array_length(split(id_viagem, '_')) - 1)
                                            ]
                                        )
                                    ),
                                    'America/Sao_Paulo'
                                )
                        ),
                        "3000-01-01"
                    ),
                    "'"
                ) as data_viagem
            from {{ staging_viagem_informada_rioonibus }}
            where {{ incremental_filter }}

            union distinct

            select distinct
                concat(
                    "'",
                    coalesce(
                        extract(date from datetime_partida),
                        extract(
                            date
                            from
                                datetime(
                                    timestamp(
                                        safe.parse_datetime(
                                            '%Y%m%d%H%M%S',
                                            split(id_viagem, '_')[
                                                safe_offset(array_length(split(id_viagem, '_')) - 1)
                                            ]
                                        )
                                    ),
                                    'America/Sao_Paulo'
                                )
                        ),
                        "3000-01-01"
                    ),
                    "'"
                ) as data_viagem
            from {{ staging_viagem_informada_brt }}
            where {{ incremental_filter }}

            union distinct

            select distinct
                concat(
                    "'",
                    extract(
                        date
                        from datetime_partida
                    ),
                    "'"
                ) as data_viagem
            from {{ staging_viagem_informada_maxtrack }}
            where {{ incremental_filter }}
        {% endset %}

        {% set partitions = (
            run_query(partitions_query).columns[0].values()
            | select
            | list
            | sort
        ) %}

        {% if partitions | length > 0 %}
            {% set gtfs_feeds_query %}
                select distinct concat("'", feed_start_date, "'") as feed_start_date
                from {{ calendario }}
                where data in ({{ partitions | join(", ") }})
            {% endset %}

            {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
        {% else %} {% set gtfs_feeds = [] %}
        {% endif %}
    {% endif %}

{% endif %}

with
    staging_rioonibus as (
        select
            id_viagem,
            cast(null as int64) as sequencial_viagem,
            id_viagem_planejada,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            cast(null as int64) as direction_id,
            cast(null as string) as tipo_viagem,
            cast(null as string) as tipo_execucao_viagem,
            fornecedor as fonte_gps,
            datetime_processamento,
            datetime_captura
        from {{ staging_viagem_informada_rioonibus }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    staging_brt as (
        select
            id_viagem,
            cast(null as int64) as sequencial_viagem,
            id_viagem_planejada,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            cast(null as int64) as direction_id,
            cast(null as string) as tipo_viagem,
            cast(null as string) as tipo_execucao_viagem,
            "brt" as fonte_gps,
            datetime_processamento,
            datetime_captura
        from {{ staging_viagem_informada_brt }}
        where
            {% if is_incremental() %} {{ incremental_filter }} and {% endif %}
            datetime_processamento >= "2024-09-10 13:00:00"
    ),
    staging_maxtrack as (
        select
            id_viagem,
            sequencial_viagem,
            id_viagem_planejada,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            direction_id,
            tipo_viagem,
            tipo_execucao_viagem,
            fornecedor as fonte_gps,
            datetime_processamento,
            datetime_captura
        from {{ staging_viagem_informada_maxtrack }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    staging_union as (
        select *
        from staging_rioonibus

        union all

        select *
        from staging_brt

        union all

        select *
        from staging_maxtrack
    ),
    staging as (
        select
            extract(date from datetime_partida) as data,
            id_viagem,
            id_viagem_planejada,
            sequencial_viagem,
            datetime_partida,
            datetime_chegada,
            datetime_processamento,
            servico,
            route_id,
            trip_id,
            shape_id,
            direction_id,
            sentido,
            id_veiculo,
            tipo_viagem,
            tipo_execucao_viagem,
            fonte_gps,
            datetime_captura
        from staging_union
    ),
    complete_partitions as (
        select *, 0 as priority
        from staging

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select
                * except (modo, versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    deduplicado as (
        select * except (priority)
        from complete_partitions
        qualify
            row_number() over (
                partition by id_viagem
                order by datetime_captura desc, datetime_processamento desc, priority
            )
            = 1
    ),
    calendario as (
        select *
        from {{ calendario }}
        {% if is_incremental() %}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    routes as (
        select *
        from {{ ref("routes_gtfs") }}
        {# from `rj-smtr.gtfs.routes` #}
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    viagem_modo as (
        select
            data,
            v.id_viagem,
            v.id_viagem_planejada,
            v.sequencial_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            v.datetime_processamento,
            case
                when v.fonte_gps = 'brt'
                then 'BRT'
                when
                    r.agency_id in ("22005", "22002", "22004", "22003")
                    or regexp_contains(r.agency_id, r"^[A-Z][0-9]$")
                then 'Ônibus'
            end as modo,
            if(trim(v.servico) = '', null, v.servico) as servico,
            if(trim(v.route_id) = '', null, v.route_id) as route_id,
            if(trim(v.trip_id) = '', null, v.trip_id) as trip_id,
            if(trim(v.shape_id) = '', null, v.shape_id) as shape_id,
            v.direction_id,
            if(trim(v.sentido) = '', null, v.sentido) as sentido,
            if(trim(v.id_veiculo) = '', null, v.id_veiculo) as id_veiculo,
            v.tipo_viagem,
            v.tipo_execucao_viagem,
            if(trim(v.fonte_gps) = '', null, v.fonte_gps) as fonte_gps,
            v.datetime_captura
        from deduplicado v
        join calendario c using (data)
        left join routes r using (route_id, feed_start_date, feed_version)
    )
select
    *,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ var("version") }}' as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_modo
