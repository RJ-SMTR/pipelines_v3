{% test subsidio_viagens_atualizadas(model) -%}
    with
        viagem_completa as (
            select data, datetime_ultima_atualizacao, feed_start_date
            from
                -- rj-smtr.projeto_subsidio_sppo.viagem_completa
                {{ ref("viagem_completa") }}
            left join {{ ref("subsidio_data_versao_efetiva") }} using (data)
            where
                data >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
                and data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        ),
        sumario_servico_dia_historico as (
            select data, datetime_ultima_atualizacao
            from {{ model }}
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
        ),
        feed_info as (
            select feed_start_date, feed_update_datetime
            from {{ ref("feed_info_gtfs") }}
        )
    select distinct data
    from viagem_completa as c
    left join sumario_servico_dia_historico as h using (data)
    left join feed_info as f using (feed_start_date)
    where
        c.datetime_ultima_atualizacao > h.datetime_ultima_atualizacao
        and c.datetime_ultima_atualizacao > f.feed_update_datetime
{%- endtest %}
