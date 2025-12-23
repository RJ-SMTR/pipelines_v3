{% test check_viagem_completa(model) -%}
WITH
    data_versao_efetiva AS (
    SELECT
        *
    FROM
        -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
        {{ ref('subsidio_data_versao_efetiva') }}
    WHERE
        DATA >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
        AND DATA BETWEEN DATE("{{ var('start_date') }}")
        AND DATE("{{ var('end_date') }}")),
    viagem_completa AS (
    SELECT
        *
    FROM
        -- rj-smtr.projeto_subsidio_sppo.viagem_completa
        {{ ref('viagem_completa') }}
    WHERE
        DATA >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
        AND DATA BETWEEN DATE("{{ var('start_date') }}")
        AND DATE("{{ var('end_date') }}")),
    feed_info AS (
    SELECT
        *
    FROM
        -- rj-smtr.gtfs.feed_info
        {{ ref('feed_info_gtfs') }}
    WHERE
        feed_version IN (
        SELECT
        feed_version
        FROM
        data_versao_efetiva) )
    SELECT
    DISTINCT DATA
    FROM
    viagem_completa
    LEFT JOIN
    data_versao_efetiva AS d
    USING
    (DATA)
    LEFT JOIN
    feed_info AS i
    ON
    (DATA BETWEEN i.feed_start_date
        AND i.feed_end_date
        OR (DATA >= i.feed_start_date
        AND i.feed_end_date IS NULL))
    WHERE
    i.feed_start_date != d.feed_start_date
    OR datetime_ultima_atualizacao < feed_update_datetime
{%- endtest %}