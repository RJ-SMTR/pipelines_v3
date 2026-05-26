{% test check_viagem_completa(model) -%}
    with
        data_versao_efetiva as (
            select *
            from
                -- rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
                {{ ref("subsidio_data_versao_efetiva") }}
            where
                data >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
                and data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        ),
        viagem_completa as (
            select *
            from
                -- rj-smtr.projeto_subsidio_sppo.viagem_completa
                {{ ref("viagem_completa") }}
            where
                data >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
                and data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        ),
        feed_info as (
            select *
            from
                -- rj-smtr.gtfs.feed_info
                {{ ref("feed_info_gtfs") }}
            where feed_version in (select feed_version from data_versao_efetiva)
        )
    select distinct data
    from viagem_completa
    left join data_versao_efetiva as d using (data)
    left join
        feed_info as i
        on (
            data between i.feed_start_date and i.feed_end_date
            or (data >= i.feed_start_date and i.feed_end_date is null)
        )
    where
        i.feed_start_date != d.feed_start_date
        or datetime_ultima_atualizacao < feed_update_datetime
{%- endtest %}
