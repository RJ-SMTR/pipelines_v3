{% test check_trips_processing(model) -%}
SELECT
    s.data,
    s.tipo_dia,
    s.subtipo_dia,
    s.tipo_os,
    s.feed_version,
    s.feed_start_date AS feed_start_date_invalido,
    i.feed_start_date AS feed_start_date_valido,
FROM (
    SELECT
        *
    FROM
        {{ ref('subsidio_data_versao_efetiva') }}
        -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
    WHERE
        DATA >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
        AND DATA BETWEEN DATE("{{ var('start_date') }}")
            AND DATE("{{ var('end_date') }}")
    ) AS s
LEFT JOIN
    -- rj-smtr.gtfs.feed_info AS i
    {{ ref('feed_info') }} AS i
ON
(DATA BETWEEN i.feed_start_date
    AND i.feed_end_date
    OR (DATA >= i.feed_start_date
    AND i.feed_end_date IS NULL))
WHERE
    i.feed_start_date != s.feed_start_date
{%- endtest %}