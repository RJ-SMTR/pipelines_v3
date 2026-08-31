{% test unique_surrogate_key(model, columns) %}

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
