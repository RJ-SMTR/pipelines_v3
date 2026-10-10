{% test accepted_values(model, column_name, values, quote=True) %}
    {{
        config(
            description="Todos os valores da coluna `"
            ~ column_name
            ~ "` estão entre os aceitos: "
            ~ values
            | join(", ")
        )
    }}
    {{
        return(
            adapter.dispatch("test_accepted_values", "dbt")(
                model, column_name, values, quote=quote
            )
        )
    }}
{% endtest %}
