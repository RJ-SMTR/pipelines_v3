{% macro clear_recce_pr_schemas(schema_prefix) %}
    {% set schema_prefix_pattern = schema_prefix ~ "__" %}
    {% set schemas = [] %}

    {% for node in graph.nodes.values() %}
        {% if (
            node.resource_type in ["model", "seed", "snapshot"]
            and node.config.enabled
            and node.schema.startswith(schema_prefix_pattern)
        ) %}
            {% do schemas.append(node.schema) %}
        {% endif %}
    {% endfor %}

    {% if execute %}
        {% for schema_name in schemas | unique %}
            {% set drop_schema_sql = (
                "drop schema if exists `"
                ~ target.project
                ~ "."
                ~ schema_name
                ~ "` cascade"
            ) %}
            {% do run_query(drop_schema_sql) %}
        {% endfor %}
    {% endif %}
{% endmacro %}
