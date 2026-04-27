{% macro table_exists(reference) %}
    {% set relation = adapter.get_relation(
        database=reference.database,
        schema=reference.schema,
        identifier=reference.identifier,
    ) %}

    {% if relation is not none %} {{ return(True) }}
    {% else %} {{ return(False) }}
    {% endif %}
{% endmacro %}
