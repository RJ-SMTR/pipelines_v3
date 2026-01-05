{% test teto_viagens(model) -%}
    with
        planejado as (
            select distinct
                data,
                servico,
                faixa_horaria_inicio,
                faixa_horaria_fim,
                partidas_total_planejada
            from {{ ref("viagem_planejada") }}
            -- from `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
            where
                data between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
                and (distancia_total_planejada > 0 or distancia_total_planejada is null)
                and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
                and data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
        ),
        viagens_remuneradas as (
            select
                r.data, r.servico, faixa_horaria_inicio, indicador_viagem_dentro_limite
            from {{ model }} r
            -- `rj-smtr-dev.abr_reprocessamento__dashboard_subsidio_sppo.viagens_remuneradas` r
            left join
                {{ ref("viagem_completa") }} c
                -- `rj-smtr.projeto_subsidio_sppo.viagem_completa` c
                using (data, id_viagem)
            left join
                planejado p
                on p.data = c.data
                and r.servico = p.servico
                and c.datetime_partida
                between faixa_horaria_inicio and faixa_horaria_fim
            where
                r.data between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
        ),
        viagens as (
            select
                * except (indicador_viagem_dentro_limite),

                countif(
                    indicador_viagem_dentro_limite is true
                ) as viagens_dentro_limite,
                count(*) as viagens_total
            from viagens_remuneradas
            group by 1, 2, 3
        )
    select *
    from viagens
    left join planejado using (data, servico, faixa_horaria_inicio)
    where
        -- caso de mais viagens do que o planejado (expectativa de
        -- viagens_dentro_limite >= partidas_total_planejada)
        (
            viagens_total >= partidas_total_planejada
            and viagens_dentro_limite < partidas_total_planejada
        )
        -- caso de menos viagens do que o planejado (expectativa de
        -- viagens_dentro_limite = viagens_total)
        or (
            viagens_total <= partidas_total_planejada
            and viagens_dentro_limite != viagens_total
        )
{% endtest %}
