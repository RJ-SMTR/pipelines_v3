{% test sequential_order(model, column_name, order_column_name) %}
    select *
    from
        (
            select
                {{ column_name }},
                {{ order_column_name }},
                max({{ order_column_name }}) over (
                    order by {{ column_name }}
                    rows between unbounded preceding and 1 preceding
                ) as max_{{ order_column_name }}_anterior
            from {{ model }}
        )
    where {{ order_column_name }} < max_{{ order_column_name }}_anterior
{% endtest %}
