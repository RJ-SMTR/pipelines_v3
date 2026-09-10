{% test not_null_planned_services(model, column_name) -%}
    {% set incremental_filter %}
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
    {% endset %}

    with
        planejados as (
            select distinct
                data,
                tipo_dia,
                consorcio,
                servico,
                sentido,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                distancia_total_planejada as km_planejada
            from {{ ref("viagem_planejada") }}
            where
                {{ incremental_filter }}
                and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                and distancia_total_planejada > 0

            union all

            select distinct
                data,
                tipo_dia,
                consorcio,
                servico,
                sentido,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                quilometragem as km_planejada
            from {{ ref("servico_planejado_faixa_horaria") }}
            where
                {{ incremental_filter }}
                and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                and quilometragem > 0
        ),
        servicos_validos as (select distinct servico from planejados)

    select *
    from {{ model }}
    where
        {{ column_name }} is null and servico in (select servico from servicos_validos)
{%- endtest %}
