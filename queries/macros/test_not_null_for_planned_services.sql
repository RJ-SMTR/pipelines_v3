{% test not_null_for_planned_services(model, column_name) -%}
    with
        servicos_validos as (
            select distinct servico
            from {{ ref("viagem_planejada") }}
            where
                data
                between '{{ var("date_range_start") }}'
                and '{{ var("date_range_end") }}'
        )

    select *
    from {{ model }}
    where
        {{ column_name }} is null and servico in (select servico from servicos_validos)
{% endtest %}
