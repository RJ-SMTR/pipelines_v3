{% test unique_surrogate_key(model, columns) %}

    {% if columns | length == 0 %}
        {{
            exceptions.raise_compiler_error(
                "The columns argument must contain at least one column."
            )
        }}
    {% endif %}

    {% set surrogate_key = dbt_utils.generate_surrogate_key(columns) %}

    with
        validation_errors as (
            select {{ surrogate_key }} as surrogate_key, count(*) as n_records
            from {{ model }}
            group by surrogate_key
            having count(*) > 1
        )

    select *
    from validation_errors

{% endtest %}
