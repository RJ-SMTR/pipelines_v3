{% macro list_columns() %}
    {% set relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.identifier
    ) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set column_names = columns | map(attribute="name") | list %}
    {{ return(column_names) }}
{% endmacro %}
