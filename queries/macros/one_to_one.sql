{% test one_to_one(model, column_name, partition_column, to_table) %}
    {% if execute %}
        {% set model_max_partition = (
            run_query("SELECT MAX(" ~ partition_column ~ ") FROM " ~ model)
            .columns[0]
            .values()[0]
        ) %}
        {% set to_table_max_partition = (
            run_query("SELECT MAX(" ~ partition_column ~ ") FROM " ~ to_table)
            .columns[0]
            .values()[0]
        ) %}
    {% endif %}
    with
        t as (
            select m.{{ column_name }} from_col, n.{{ column_name }} to_col
            from
                (
                    select {{ column_name }}
                    from {{ model }}
                    where {{ partition_column }} = "{{model_max_partition}}"
                ) m
            left join
                (
                    select {{ column_name }}
                    from {{ to_table }}
                    where {{ partition_column }} = "{{to_table_max_partition}}"
                ) n
                on m.{{ column_name }} = n.{{ column_name }}
        )
    select *
    from (select from_col, count(to_col) ct from t group by from_col)
    where ct != 1
{% endtest %}
