{% test unique_by_date(model, column_name, date_column_name) %}
    select *
    from
        (
            select
                {{ column_name }},
                {{ date_column_name }},
                row_number() over (
                    partition by {{ column_name }}, {{ date_column_name }}
                ) rn
            from {{ model }}
            where
                {{ date_column_name }}
                = (select max({{ date_column_name }}) from {{ model }})
        )

    where rn > 1
{% endtest %}
