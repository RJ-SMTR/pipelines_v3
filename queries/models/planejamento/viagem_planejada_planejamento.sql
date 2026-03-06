{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        alias="viagem_planejada",
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set source_filter %}
    data between
        date_sub(date('{{ var("date_range_start") }}'), interval 1 day)
        and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
{% if execute and is_incremental() %}
    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            concat(
                {% for c in columns %}
                    {% if c == "trajetos_alternativos" %}ifnull(to_json_string({{ c }}), 'n/a')
                    {% else %}ifnull(cast({{ c }} as string), 'n/a')
                    {% endif %}
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        )
    {% endset %}
    {% set gtfs_feeds_query %}
        select distinct concat("'", feed_start_date, "'") as feed_start_date
        from {{ calendario }}
        where {{ source_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    trips_dia as (
        select *
        from {{ ref("aux_trips_dia") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
                and {{ source_filter }}
            {% endif %}
    ),
    frequencies_tratada as (
        select *
        from {{ ref("aux_frequencies_horario_tratado") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% endif %}
    ),
    trips_frequences_dia as (
        select
            td.*,
            timestamp(data + start_time, "America/Sao_Paulo") as start_timestamp,
            timestamp(data + end_time, "America/Sao_Paulo") as end_timestamp,
            f.headway_secs
        from trips_dia td
        join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
    ),
    trips_alternativas as (
        select
            data,
            servico,
            direction_id,
            array_agg(
                struct(
                    trip_id as trip_id,
                    shape_id as shape_id,
                    evento as evento,
                    extensao as extensao
                )
            ) as trajetos_alternativos
        from trips_dia td
        where td.trip_id not in (select trip_id from frequencies_tratada)
        group by 1, 2, 3
    ),
    viagens_frequencies as (
        select
            tfd.* except (start_timestamp, end_timestamp, headway_secs),
            datetime(partida, "America/Sao_Paulo") as datetime_partida
        from
            trips_frequences_dia tfd,
            unnest(
                generate_timestamp_array(
                    start_timestamp,
                    timestamp_sub(end_timestamp, interval 1 second),
                    interval headway_secs second
                )
            ) as partida
    ),
    viagens_stop_times as (
        select
            td.data,
            trip_id,
            td.modo,
            td.route_id,
            td.service_id,
            td.servico,
            td.direction_id,
            td.shape_id,
            td.tipo_dia,
            td.subtipo_dia,
            td.tipo_os,
            feed_version,
            feed_start_date,
            td.evento,
            td.extensao,
            td.distancia_total_planejada,
            td.indicador_possui_os,
            td.horario_inicio,
            td.horario_fim,
            td.data + st.arrival_time as datetime_partida
        from trips_dia td
        join
            {{ ref("aux_stop_times_horario_tratado") }} st using (
                feed_start_date, feed_version, trip_id
            )
        left join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% endif %}
            and st.stop_sequence = 0
            and f.trip_id is null
    ),
    viagens_trips_alternativas as (
        select v.*, ta.trajetos_alternativos
        from
            (
                select *
                from viagens_frequencies
                union all
                select *
                from viagens_stop_times
            ) v
        left join trips_alternativas ta using (data, servico, direction_id)
    ),
    viagem_filtrada as (
        -- filtra viagens fora do horario de inicio e fim e em dias não previstos na OS
        select *
        from viagens_trips_alternativas
        where
            (distancia_total_planejada is null or distancia_total_planejada > 0)
            and (
                not indicador_possui_os
                or horario_inicio is null
                or horario_fim is null
                or datetime_partida between data + horario_inicio and data + horario_fim
            )
    ),
    servico_circular as (
        select feed_start_date, feed_version, shape_id
        {# from `rj-smtr.planejamento.shapes_geom` #}
        from {{ ref("shapes_geom_planejamento") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% endif %}
            and round(st_y(start_pt), 4) = round(st_y(end_pt), 4)
            and round(st_x(start_pt), 4) = round(st_x(end_pt), 4)
    ),
    viagem_planejada as (
        select
            date(datetime_partida) as data,
            datetime_partida,
            modo,
            service_id,
            trip_id,
            route_id,
            shape_id,
            servico,
            case
                when c.shape_id is not null
                then "C"
                when direction_id = '0'
                then "I"
                else "V"
            end as sentido,
            evento,
            extensao,
            trajetos_alternativos,
            data as data_referencia,
            tipo_dia,
            subtipo_dia,
            tipo_os,
            feed_version,
            feed_start_date
        from viagem_filtrada v
        left join servico_circular c using (shape_id, feed_version, feed_start_date)
    ),
    viagem_planejada_id as (
        select
            *,
            concat(
                servico,
                "_",
                sentido,
                "_",
                shape_id,
                "_",
                format_datetime("%Y%m%d%H%M%S", datetime_partida)
            ) as id_viagem
        from viagem_planejada
    ),
    dados_novos as (
        select data, id_viagem, * except (data, id_viagem, rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_viagem order by data_referencia desc
                    ) as rn
                from viagem_planejada_id
            )
        where
            rn = 1 and data is not null
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    {% if is_incremental() %}
        dados_atuais as (select * from {{ this }} where {{ incremental_filter }}),
    {% endif %}
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                id_viagem,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais
        {% else %}
            select
                cast(null as string) as id_viagem,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, {{ sha_column }} as sha_dado_novo, a.* except (id_viagem)
        from dados_novos n
        left join sha_dados_atuais a using (id_viagem)
    ),
    colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual
            ),
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    )
select *
from colunas_controle
