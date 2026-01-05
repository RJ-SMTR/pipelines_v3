{% test teto_pagamento_valor_subsidio_pago(model, table_id, schema, expression) -%}
WITH
{{ table_id }} AS (
    SELECT
        *,
    FROM
        {{ model }}
    WHERE
        DATA BETWEEN DATE("{{ var('date_range_start') }}")
        AND DATE("{{ var('date_range_end') }}")),
subsidio_valor_km_tipo_viagem AS (
    SELECT
        data_inicio,
        data_fim,
        MAX(subsidio_km) AS subsidio_km_teto
    FROM
        -- `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
        {{ ref('subsidio_valor_km_tipo_viagem') }}
    WHERE
        subsidio_km > 0
    GROUP BY
        1,
        2)
SELECT
    *
FROM
    {{ table_id }} AS s
LEFT JOIN
    subsidio_valor_km_tipo_viagem AS p
ON
    s.data BETWEEN p.data_inicio
    AND p.data_fim
WHERE
    NOT({{ expression }})
{%- endtest %}