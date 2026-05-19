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

-- depends_on: {{ ref('feed_info_gtfs') }}
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
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
    {% set feed_filter %}
        feed_start_date in (
            date('{{ last_feed_version }}'), date('{{ var("data_versao_gtfs") }}')
        )
    {% endset %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
    {% set feed_filter %} 1 = 0 {% endset %}
{% endif %}

with
    trips as (
        select *
        from {{ ref("aux_trips") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    frequencies_tratada as (
        select *
        from {{ ref("aux_frequencies_horario_tratado") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    trips_frequencies as (
        select t.*, f.start_seconds, f.end_seconds, f.headway_secs
        from trips t
        join frequencies_tratada f using (feed_start_date, feed_version, trip_id)
    ),
    trajeto_alternativo_sentido as (
        select
            feed_start_date,
            feed_version,
            servico,
            tipo_os,
            evento,
            case when sentido = 'V' then '1' else '0' end as direction_id,
            extensao
        from
            (
                select
                    feed_start_date,
                    feed_version,
                    servico,
                    tipo_os,
                    evento,
                    extensao_ida,
                    extensao_volta
                from {{ ref("ordem_servico_trajeto_alternativo_gtfs") }}
                where
                    feed_start_date < date('{{ var("DATA_GTFS_V4_INICIO") }}')
                    {% if is_incremental() %} and {{ feed_filter }} {% endif %}
            ) unpivot (
                (extensao)
                for sentido in ((extensao_ida) as 'I', (extensao_volta) as 'V')
            )
        union all
        select
            feed_start_date,
            feed_version,
            servico,
            tipo_os,
            evento,
            case when left(sentido, 1) = 'V' then '1' else '0' end as direction_id,
            extensao
        from {{ ref("ordem_servico_trajeto_alternativo_sentido") }}
        where
            feed_start_date >= date('{{ var("DATA_GTFS_V4_INICIO") }}')
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
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
        where st.stop_sequence = 0 and f.trip_id is null
    ),
    viagens_unidas as (
        select *
        from viagens_frequencies
        union all
        select *
        from viagens_stop_times
    ),
    ordem_servico_extensao as (
        select
            feed_start_date, feed_version, servico, tipo_os, tipo_dia, sentido, extensao
        from {{ ref("aux_ordem_servico_diaria") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    servico_circular as (
        select feed_start_date, feed_version, shape_id
        from {{ ref("shapes_geom_planejamento") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
            and round(st_y(start_pt), 4) = round(st_y(end_pt), 4)
            and round(st_x(start_pt), 4) = round(st_x(end_pt), 4)
    ),
    viagem_planejada_base as (
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
            case
                when service_id like "%U_%"
                then "Dia Útil"
                when service_id like "%S_%"
                then "Sabado"
                when service_id like "%D_%"
                then "Domingo"
                else service_id
            end as tipo_dia,
            trip_id,
            route_id,
            shape_id,
            servico,
            direction_id,
            case
                when c.shape_id is not null
                then "C"
                when direction_id = '0'
                then "I"
                else "V"
            end as sentido,
            evento,
            feed_version,
            feed_start_date
        from viagens_unidas v
        left join servico_circular c using (shape_id, feed_version, feed_start_date)
    ),
    viagem_planejada_os as (
        select
            v.* except (tipo_dia),
            coalesce(ose.tipo_dia, v.tipo_dia) as tipo_dia,
            ose.tipo_os,
            ose.extensao
        from viagem_planejada_base v
        left join
            ordem_servico_extensao ose
            on ose.feed_start_date = v.feed_start_date
            and ose.feed_version = v.feed_version
            and ose.servico = v.servico
            and ose.sentido = v.sentido
            and (
                ose.tipo_dia = v.tipo_dia
                or (ose.tipo_dia = 'Ponto Facultativo' and v.tipo_dia = 'Dia Útil')
            )
    ),
    trips_alternativas as (
        select
            t.feed_start_date,
            t.feed_version,
            t.servico,
            t.direction_id,
            tas.tipo_os,
            array_agg(
                if(
                    tas.extensao is not null and tas.extensao != 0,
                    struct(
                        t.trip_id as trip_id,
                        t.shape_id as shape_id,
                        t.evento as evento,
                        tas.extensao as extensao
                    ),
                    struct(
                        cast(null as string) as trip_id,
                        cast(null as string) as shape_id,
                        cast(null as string) as evento,
                        cast(null as float64) as extensao
                    )
                )
            ) as trajetos_alternativos
        from trips t
        left join
            trajeto_alternativo_sentido tas
            on t.feed_start_date = tas.feed_start_date
            and t.feed_version = tas.feed_version
            and t.servico = tas.servico
            and t.evento = tas.evento
            and t.direction_id = tas.direction_id
        where t.trip_id not in (select trip_id from frequencies_tratada)
        group by 1, 2, 3, 4, 5
    ),
    viagem_planejada_completa as (
        select v.*, ta.trajetos_alternativos
        from viagem_planejada_os v
        left join
            trips_alternativas ta using (
                feed_start_date, feed_version, servico, direction_id, tipo_os
            )
    ),
    viagem_planejada_id as (
        select
            * except (direction_id),
            concat(
                servico,
                "_",
                sentido,
                "_",
                shape_id,
                "_",
                service_id,
                "_",
                coalesce(tipo_os, ''),
                "_",
                tipo_dia,
                "_",
                replace(horario_partida, ':', '')
            ) as id_viagem
        from viagem_planejada_completa
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
        dados_atuais as (select * from {{ this }} where {{ feed_filter }}),
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
