{% test not_null(model, column_name) %}
    {{
        config(
            description="Todos os valores da coluna `" ~ column_name ~ "` não nulos"
        )
    }}
    {{ return(adapter.dispatch("test_not_null", "dbt")(model, column_name)) }}
{% endtest %}
