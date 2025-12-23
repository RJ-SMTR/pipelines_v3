{% test test_completude_temperatura_inmet(model) %}

    with
        validation as (
            select data, count(distinct hora) as qtd
            from {{ model }}
            where
                id_estacao in ('A621', 'A652', 'A636', 'A602')
                and temperatura is not null
            group by data
        )

    select *
    from validation
    where qtd != 24

{% endtest %}
