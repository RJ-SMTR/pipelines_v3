{% test test_completude_temperatura_inmet(model) %}
    {% set data_fim_ajustada %}
        if(
            date_diff(
                date('{{ var("date_range_end") }}'),
                date('{{ var("date_range_start") }}'),
                day
            ) = 1,
            date_sub(date('{{ var("date_range_end") }}'), interval 1 day),
            date('{{ var("date_range_end") }}')
        )
    {% endset %}

    with
        ajustado as (
            select
                case
                    when data >= date("{{ var('DATA_SUBSIDIO_V23_INICIO') }}")
                    then
                        extract(
                            date
                            from datetime_sub(datetime(data, hora), interval 1 hour)
                        )
                    else data
                end as data,
                case
                    when data >= date("{{ var('DATA_SUBSIDIO_V23_INICIO') }}")
                    then
                        extract(
                            hour
                            from datetime_sub(datetime(data, hora), interval 1 hour)
                        )
                    else extract(hour from hora)
                end as hora,
                temperatura,
                id_estacao
            from {{ model }}
            where
                data between date('{{ var("date_range_start") }}') and date_add(
                    {{ data_fim_ajustada }}, interval 1 day
                )
        ),
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
            from ajustado
            where
                id_estacao in ('A621', 'A652', 'A636', 'A602')
                and temperatura is not null
                and data between date(
                    '{{ var("date_range_start") }}'
                ) and {{ data_fim_ajustada }}
            group by data
        )
    select e.data, v.qtd
    from datas_esperadas e
    left join validation v using (data)
    where v.qtd is null or v.qtd != 24
{% endtest %}
