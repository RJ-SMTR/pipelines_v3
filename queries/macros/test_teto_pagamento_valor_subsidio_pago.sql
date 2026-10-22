{% test teto_pagamento_valor_subsidio_pago(model, table_id, schema, expression) -%}
    with
        {{ table_id }} as (
            select *,
            from {{ model }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        subsidio_valor_km_tipo_viagem as (
            select data_inicio, data_fim, max(subsidio_km) as subsidio_km_teto
            from
                -- `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
                {{ ref("subsidio_valor_km_tipo_viagem") }}
            where subsidio_km > 0
            group by 1, 2
        )
    select *
    from {{ table_id }} as s
    left join
        subsidio_valor_km_tipo_viagem as p
        on s.data between p.data_inicio and p.data_fim
    where not ({{ expression }})
{%- endtest %}
