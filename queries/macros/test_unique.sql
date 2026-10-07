{% test unique(model, column_name) %}
    {{
        config(
            description="Todos os valores da coluna `" ~ column_name ~ "` são únicos"
        )
    }}
    {{ return(adapter.dispatch("test_unique", "dbt")(model, column_name)) }}
{% endtest %}
