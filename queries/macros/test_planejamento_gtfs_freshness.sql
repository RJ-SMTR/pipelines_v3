{% test planejamento_gtfs_freshness(model, partition_column, model_relation) -%}
    with
        feed_mais_recentemente_atualizado as (
            select feed_start_date, feed_end_date, feed_update_datetime
            from {{ ref("feed_info_gtfs") }}
            qualify
                row_number() over (
                    order by feed_update_datetime desc, feed_start_date desc
                )
                = 1
        ),
        particoes_esperadas as (
            {% if partition_column == "data" %}
                select data as partition_date, feed_start_date, feed_update_datetime
                from
                    feed_mais_recentemente_atualizado,
                    unnest(
                        generate_date_array(
                            feed_start_date,
                            coalesce(
                                feed_end_date,
                                date_add(
                                    greatest(
                                        current_date("America/Sao_Paulo"),
                                        feed_start_date
                                    ),
                                    interval 2 day
                                )
                            )
                        )
                    ) as data
            {% else %}
                select
                    feed_start_date as partition_date,
                    feed_start_date,
                    feed_update_datetime
                from feed_mais_recentemente_atualizado
            {% endif %}
        ),
        limites as (
            select min(partition_date) as data_inicio, max(partition_date) as data_fim
            from particoes_esperadas
        ),
        particoes_modelo as (
            select distinct {{ partition_column }} as partition_date, feed_start_date
            from {{ model }}
            where
                {{ partition_column }} between (select data_inicio from limites) and (
                    select data_fim from limites
                )
        ),
        metadados_particoes as (
            select
                parse_date("%Y%m%d", partition_id) as partition_date,
                max(
                    datetime(last_modified_time, "America/Sao_Paulo")
                ) as partition_update_datetime
            from
                `{{ model_relation.database }}.{{ model_relation.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ model_relation.identifier }}"
                and partition_id not in ("__NULL__", "__UNPARTITIONED__")
                and parse_date(
                    "%Y%m%d",
                    partition_id
                ) between (select data_inicio from limites) and (
                    select data_fim from limites
                )
            group by 1
        )
    select
        e.partition_date,
        e.feed_start_date as feed_start_date_esperado,
        e.feed_update_datetime,
        m.partition_update_datetime,
        countif(p.feed_start_date = e.feed_start_date) as registros_feed_esperado
    from particoes_esperadas as e
    left join particoes_modelo as p using (partition_date)
    left join metadados_particoes as m using (partition_date)
    group by 1, 2, 3, 4
    having
        registros_feed_esperado = 0
        or partition_update_datetime is null
        or partition_update_datetime < feed_update_datetime
{%- endtest %}
