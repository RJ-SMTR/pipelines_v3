{% test unique_relation(model, column_name, partition_column, relation_field) %}
    select *
    from
        (
            select
                {{ column_name }},
                {{ partition_column }},
                count({{ relation_field }}) ct
            from {{ model }}
            where
                {{ partition_column }}
                = (select max({{ partition_column }}) from {{ model }})
            group by 1, 2
        )
    where ct != 1
{% endtest %}
