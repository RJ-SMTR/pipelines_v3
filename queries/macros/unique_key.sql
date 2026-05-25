{% test unique_key(model, column_name, partition_column, combined_keys) %}
    select *
    from
        (
            select
                {{ column_name }},
                {{ partition_column }},
                row_number() over (
                    partition by
                        {{
                            (
                                (column_name ~ "," ~ combined_keys | join(","))
                                if combined_keys != ""
                                else column_name
                            )
                        }}
                ) rn
            from {{ model }}
            where
                {{ partition_column }}
                = (select max({{ partition_column }}) from {{ model }})
        )

    where rn > 1
{% endtest %}
