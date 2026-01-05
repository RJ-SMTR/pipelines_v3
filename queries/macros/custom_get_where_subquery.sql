{% macro get_where_subquery(relation) -%}
    {% set where = config.get("where") %}
    {% if where %}
        {% set re = modules.re %}
        {% set final_where = namespace(value=where) %}

        {% set matches = re.finditer("\{([^}]+)\}", where) %}

        {% for match in matches %}
            {% set var_name = match.group(1) %}
            {% set var_replace = "{" ~ var_name ~ "}" %}
            {% if var_name == "partitions" and var(var_name) == "" %}
                {% set partition_query %}
                    select
                        concat("'", parse_date("%Y%m%d", partition_id), "'") as particao
                    from
                        {{relation.database}}.{{relation.schema}}.INFORMATION_SCHEMA.PARTITIONS
                    where
                        table_name = "{{relation.identifier}}"
                        and partition_id != "__NULL__"
                        and datetime(last_modified_time, "America/Sao_Paulo") >= datetime("{{var('date_range_start')}}")
                        and datetime(last_modified_time, "America/Sao_Paulo") < datetime("{{var('date_range_end')}}")
                {% endset %}

                {% set partitions = run_query(partition_query).columns[0].values() %}
                {% set var_value = partitions | join(", ") %}
            {% else %} {% set var_value = var(var_name) %}
            {% endif %}
            {% set final_where.value = final_where.value.replace(
                var_replace, var_value
            ) %}
        {% endfor %}
        (select * from {{ relation }} where {{ final_where.value }})
    {%- else -%} {{ relation }}
    {%- endif -%}
{%- endmacro %}
