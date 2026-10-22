{% test contained(model, column_name, in, field) %}
    select *
    from
        (
            select distinct {{ column_name }} contained, _contains
            from {{ model }}
            left join
                (select distinct {{ field }} _contains from {{ in }})
                on {{ column_name }} = _contains
        )
    where _contains is null
{% endtest %}
