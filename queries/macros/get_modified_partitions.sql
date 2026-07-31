{% macro get_modified_partitions(
    source_relation, include_adjacent=false, max_age_days=5
) %}
    {% if not execute %} {{ return([]) }} {% endif %}

    {% set partitions_query %}
        with
            modified as (
                select parse_date("%Y%m%d", partition_id) as data
                from `{{ source_relation.database }}.{{ source_relation.schema }}.INFORMATION_SCHEMA.PARTITIONS`
                where
                    table_name = "{{ source_relation.identifier }}"
                    and partition_id not in ("__NULL__", "__UNPARTITIONED__")
                    and datetime(last_modified_time, "America/Sao_Paulo")
                    >= datetime("{{ var('date_range_start') }}")
                    and parse_date("%Y%m%d", partition_id) >= date_sub(
                        current_date("America/Sao_Paulo"), interval {{ max_age_days }} day
                    )
            )
            {% if include_adjacent %}
                ,
                expanded as (
                    select distinct d as data
                    from
                        modified,
                        unnest(
                            [
                                data,
                                date_sub(data, interval 1 day),
                                date_add(data, interval 1 day)
                            ]
                        ) as d
                )
                select concat("'", data, "'") as data
                from expanded
            {% else %}
                select concat("'", data, "'") as data
                from modified
            {% endif %}
    {% endset %}

    {% set result = run_query(partitions_query) %}
    {% if result.columns[0].values() | length > 0 %}
        {{ return(result.columns[0].values()) }}
    {% else %} {{ return([]) }}
    {% endif %}
{% endmacro %}
