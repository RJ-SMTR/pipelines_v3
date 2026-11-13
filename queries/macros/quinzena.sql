{% macro inicio_quinzena(data_expr) %}
    if(
        extract(day from {{ data_expr }}) <= 15,
        date_trunc({{ data_expr }}, month),
        date_add(date_trunc({{ data_expr }}, month), interval 15 day)
    )
{% endmacro %}

{% macro fim_quinzena(data_expr) %}
    if(
        extract(day from {{ data_expr }}) <= 15,
        date_add(date_trunc({{ data_expr }}, month), interval 14 day),
        last_day({{ data_expr }}, month)
    )
{% endmacro %}
