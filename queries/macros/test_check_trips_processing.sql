{% test check_trips_processing(model) -%}
    select
        s.data,
        s.tipo_dia,
        s.subtipo_dia,
        s.tipo_os,
        s.feed_version,
        s.feed_start_date as feed_start_date_invalido,
        i.feed_start_date as feed_start_date_valido,
    from
        (
            select *
            from {{ ref("subsidio_data_versao_efetiva") }}
            -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
            where
                data >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
                and data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        ) as s
    left join
        -- rj-smtr.gtfs.feed_info AS i
        {{ ref("feed_info") }} as i
        on (
            data between i.feed_start_date and i.feed_end_date
            or (data >= i.feed_start_date and i.feed_end_date is null)
        )
    where i.feed_start_date != s.feed_start_date
{%- endtest %}
