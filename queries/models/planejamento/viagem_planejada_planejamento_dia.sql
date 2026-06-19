{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        alias="viagem_planejada_dia",
        incremental_strategy="insert_overwrite",
    )
}}

{% set date_range_end_extended %}
    date_add(date("{{ var('date_range_end') }}"), interval 2 day)
{% endset %}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and {{ date_range_end_extended }}
{% endset %}

{% set source_filter %}
    data between
        date_sub(date('{{ var("date_range_start") }}'), interval 1 day)
        and {{ date_range_end_extended }}
{% endset %}

{% set calendario = ref("calendario") %}
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
    {% set feed_filter %}
        {% if gtfs_feeds | length > 0 %}
            feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% else %} 1 = 0
        {% endif %}
    {% endset %}
{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    calendario as (
        select
            data,
            tipo_dia,
            subtipo_dia,
            tipo_os,
            service_ids,
            feed_version,
            feed_start_date
        from {{ calendario }}
        where {{ source_filter }}
    ),
    viagem_planejada as (
        select *
        from {{ ref("viagem_planejada_planejamento") }}
        where
            feed_start_date >= '{{ var("feed_inicial_viagem_planejada") }}'
            {% if is_incremental() %} and {{ feed_filter }} {% endif %}
    ),
    ordem_servico as (
        select
            feed_start_date,
            feed_version,
            servico,
            sentido,
            tipo_dia,
            tipo_os,
            distancia_total_planejada,
            horario_inicio,
            horario_fim
        from {{ ref("aux_ordem_servico_horario_tratado") }}
    ),
    viagem_dia as (
        select
            date(
                datetime(
                    timestamp(
                        c.data + make_interval(
                            hour => cast(split(vp.horario_partida, ':')[0] as int64),
                            minute => cast(split(vp.horario_partida, ':')[1] as int64),
                            second => cast(split(vp.horario_partida, ':')[2] as int64)
                        ),
                        "America/Sao_Paulo"
                    ),
                    "America/Sao_Paulo"
                )
            ) as data,
            datetime(
                timestamp(
                    c.data + make_interval(
                        hour => cast(split(vp.horario_partida, ':')[0] as int64),
                        minute => cast(split(vp.horario_partida, ':')[1] as int64),
                        second => cast(split(vp.horario_partida, ':')[2] as int64)
                    ),
                    "America/Sao_Paulo"
                ),
                "America/Sao_Paulo"
            ) as datetime_partida,
            vp.modo,
            vp.service_id,
            vp.trip_id,
            vp.route_id,
            vp.shape_id,
            vp.servico,
            vp.sentido,
            vp.evento,
            vp.extensao,
            vp.trajetos_alternativos,
            c.data as data_referencia,
            c.tipo_dia,
            c.subtipo_dia,
            c.tipo_os,
            os.distancia_total_planejada,
            os.feed_start_date is not null as indicador_possui_os,
            os.horario_inicio,
            os.horario_fim,
            vp.horario_partida,
            vp.id_viagem,
            vp.feed_version,
            vp.feed_start_date
        from calendario c
        join
            viagem_planejada vp
            on c.feed_start_date = vp.feed_start_date
            and vp.service_id in unnest(c.service_ids)
            and vp.tipo_dia = c.tipo_dia
            and (
                vp.tipo_os = c.tipo_os
                or vp.modo != 'Ônibus'
                or (
                    length(regexp_extract(vp.servico, r"[0-9]+")) = 4
                    and regexp_extract(servico, r"[0-9]+") like "2%"
                )
            )
        left join
            ordem_servico os
            on os.feed_start_date = c.feed_start_date
            and os.servico = vp.servico
            and os.tipo_dia = c.tipo_dia
            and os.tipo_os = vp.tipo_os
            and (
                (os.sentido in ('I', 'C') and vp.sentido in ('I', 'C'))
                or (os.sentido = 'V' and vp.sentido = 'V')
            )
    ),
    viagem_filtrada as (
        select * except (horario_inicio, horario_fim, horario_partida)
        from viagem_dia
        where
            (distancia_total_planejada is null or distancia_total_planejada > 0)
            and (
                not indicador_possui_os
                or horario_inicio is null
                or horario_fim is null
                or datetime_partida between data + horario_inicio and data + horario_fim
            )
    ),
    dados_novos as (
        select
            data,
            id_viagem,
            * except (data, id_viagem, distancia_total_planejada, indicador_possui_os)
        from viagem_filtrada
        where
            data is not null
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    {% if is_incremental() %}
        dados_atuais as (select * from {{ this }} where {{ incremental_filter }}),
    {% endif %}
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                data,
                id_viagem,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais
        {% else %}
            select
                cast(null as date) as data,
                cast(null as string) as id_viagem,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, {{ sha_column }} as sha_dado_novo, a.* except (data, id_viagem)
        from dados_novos n
        left join sha_dados_atuais a using (data, id_viagem)
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
