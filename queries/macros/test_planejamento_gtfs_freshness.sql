{% test planejamento_gtfs_freshness(model, partition_column, model_relation) -%}
    with
        feeds_atualizados as (
            select feed_start_date, feed_end_date, feed_update_datetime
            from {{ ref("feed_info_gtfs") }}
            where
                date(
                    feed_update_datetime
                ) between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        particoes_esperadas as (
            {% if partition_column == "data" %}
                select data as partition_date, feed_start_date, feed_update_datetime
                from
                    feeds_atualizados,
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
                from feeds_atualizados
            {% endif %}
        ),
        particoes_modelo as (
            select distinct {{ partition_column }} as partition_date, feed_start_date
            from {{ model }}
            where
                {{ partition_column }}
                in (select partition_date from particoes_esperadas)
        ),
        metadados_particoes as (
            select
                parse_date("%Y%m%d", partition_id) as partition_date,
                datetime(
                    last_modified_time, "America/Sao_Paulo"
                ) as partition_update_datetime
            from
                `{{ model_relation.database }}.{{ model_relation.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ model_relation.identifier }}"
                and partition_id not in ("__NULL__", "__UNPARTITIONED__")
                and parse_date("%Y%m%d", partition_id)
                in (select partition_date from particoes_esperadas)
        )
    select
        e.partition_date,
        e.feed_start_date as feed_start_date_esperado,
        e.feed_update_datetime,
        m.partition_update_datetime
    from particoes_esperadas as e
    left join
        particoes_modelo as p
        on e.partition_date = p.partition_date
        and e.feed_start_date = p.feed_start_date
    left join metadados_particoes as m on e.partition_date = m.partition_date
    where
        p.feed_start_date is null
        or m.partition_update_datetime < e.feed_update_datetime
{%- endtest %}
