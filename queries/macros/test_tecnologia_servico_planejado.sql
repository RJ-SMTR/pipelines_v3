{% test test_tecnologia_servico_planejado(model) %}

    with
        left_table as (
            select distinct servico, data
            from {{ model }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

                and servico is not null
        ),

        right_table as (
            select servico, inicio_vigencia, fim_vigencia
            from {{ ref("tecnologia_servico") }}
            where
                servico is not null
                and inicio_vigencia <= date("{{ var('date_range_end') }}")
                and (
                    fim_vigencia is null or fim_vigencia >= date
                    ("{{ var('date_range_start') }}")
                )
        ),
        exceptions as (
            select l.data, l.servico
            from left_table l
            left join
                right_table r
                on l.servico = r.servico
                and l.data between r.inicio_vigencia and coalesce(
                    r.fim_vigencia, date("{{var('DATA_SUBSIDIO_V99_INICIO')}}")
                )
            where r.servico is null
        )
    select *
    from exceptions
    order by data, servico

{% endtest %}
