{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="viagem_planejada",
        incremental_strategy="insert_overwrite",
    )
}}

-- depends_on: {{ ref('calendario') }}
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
        from {{ ref("calendario") }}
        where data between
            date_sub(date('{{ var("date_range_start") }}'), interval 1 day)
            and date('{{ var("date_range_end") }}')
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    trips as (
        select *
        from {{ ref("aux_trips") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %}
                and feed_start_date in ({{ gtfs_feeds | join(", ") }})
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
    trips_frequencies as (
        select t.*, f.start_seconds, f.end_seconds, f.headway_secs
        from trips t
        join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
    ),
    trips_alternativas as (
        select
            feed_start_date,
            feed_version,
            servico,
            direction_id,
            array_agg(
                struct(trip_id as trip_id, shape_id as shape_id, evento as evento)
            ) as trajetos_alternativos
        from trips t
        where t.trip_id not in (select trip_id from frequencies_tratada)
        group by 1, 2, 3, 4
    ),
    viagens_frequencies as (
        select
            tf.trip_id,
            tf.modo,
            tf.route_id,
            tf.service_id,
            tf.servico,
            tf.direction_id,
            tf.shape_id,
            tf.feed_version,
            tf.feed_start_date,
            tf.evento,
            partida_seconds
        from
            trips_frequencies tf,
            unnest(
                generate_array(tf.start_seconds, tf.end_seconds - 1, tf.headway_secs)
            ) as partida_seconds
        where tf.service_id != 'EXCEP'
    ),
    viagens_stop_times as (
        select
            t.trip_id,
            t.modo,
            t.route_id,
            t.service_id,
            t.servico,
            t.direction_id,
            t.shape_id,
            t.feed_version,
            t.feed_start_date,
            t.evento,
            st.arrival_seconds as partida_seconds
        from trips t
        join
            {{ ref("aux_stop_times_horario_tratado") }} st using (
                feed_start_date, feed_version, trip_id
            )
        left join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
        where st.stop_sequence = 0 and f.trip_id is null and t.service_id != 'EXCEP'
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
        left join
            trips_alternativas ta using (
                feed_start_date, feed_version, servico, direction_id
            )
    ),
    servico_circular as (
        select feed_start_date, feed_version, shape_id
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
            concat(
                lpad(cast(div(partida_seconds, 3600) as string), 2, '0'),
                ':',
                lpad(cast(mod(div(partida_seconds, 60), 60) as string), 2, '0'),
                ':',
                lpad(cast(mod(partida_seconds, 60) as string), 2, '0')
            ) as horario_partida,
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
            trajetos_alternativos,
            feed_version,
            feed_start_date
        from viagens_trips_alternativas v
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
                service_id,
                "_",
                replace(horario_partida, ':', '')
            ) as id_viagem
        from viagem_planejada
    ),
    dados_novos as (
        select id_viagem, * except (id_viagem, rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_viagem order by feed_start_date desc
                    ) as rn
                from viagem_planejada_id
            )
        where rn = 1
    ),
    {% if is_incremental() %}
        dados_atuais as (
            select *
            from {{ this }}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        ),
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
