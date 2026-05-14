{% test test_completude_temperatura(model, expected_qtd=24) %}
    {% set data_fim_ajustada %}
        least(
            date('{{ var("date_range_end") }}'),
            date_sub(current_date('America/Sao_Paulo'), interval 1 day)
        )
    {% endset %}

    with
        datas_esperadas as (
            select data
            from
                unnest(
                    generate_date_array(
                        date('{{ var("date_range_start") }}'), {{ data_fim_ajustada }}
                    )
                ) as data
        ),
        validation as (
            select data, count(distinct hora) as qtd
            from {{ model }}
            where
                temperatura is not null
                and data between date(
                    '{{ var("date_range_start") }}'
                ) and {{ data_fim_ajustada }}
            group by data
        )
    select e.data, v.qtd
    from datas_esperadas e
    left join validation v using (data)
    where v.qtd is null or v.qtd != {{ expected_qtd }}
{% endtest %}
