{% test check_partidas_planejadas(model, column_name) -%}
    with
        viagem_planejada as (
            select distinct
                data,
                tipo_dia,
                servico,
                sentido,
                faixa_horaria_inicio,
                partidas_total_planejada,
                feed_start_date
            from {{ ref("viagem_planejada") }}
            -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
            where
                data between greatest(
                    date("{{ var('date_range_start') }}"),
                    date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
                ) and date("{{ var('date_range_end') }}")
        ),
        os_faixa as (
            select
                data,
                servico,
                left(sentido, 1) as sentido,
                tipo_os,
                tipo_dia,
                datetime(data) + interval cast(
                    split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
                ) hour as faixa_horaria_inicio,
                partidas,
                feed_start_date,
            from {{ ref("subsidio_data_versao_efetiva") }}
            left join
                {{ ref("ordem_servico_faixa_horaria_sentido") }} using (
                    feed_start_date, tipo_os, tipo_dia
                )
            where
                data between greatest(
                    date("{{ var('date_range_start') }}"),
                    date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
                ) and date("{{ var('date_range_end') }}")
                and quilometragem != 0
        )

    select *
    from viagem_planejada p
    full join os_faixa using (data, servico, faixa_horaria_inicio, sentido)

    where partidas != partidas_total_planejada

{%- endtest %}
