{% test check_km_planejada(model) -%}
    with
        viagem_planejada as (
            select distinct
                data,
                servico,
                sentido,
                faixa_horaria_inicio,
                round(distancia_total_planejada, 3) as distancia_total_planejada,
                -- Para o histórico, usa os tipos do calendário.
                cast(
                    null as array<
                        struct<data_operacional date, tipo_dia string, tipo_os string>
                    >
                ) as dias_planejados
            from {{ ref("viagem_planejada") }}
            -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")

            union all

            select
                data,
                servico,
                sentido,
                faixa_horaria_inicio,
                round(quilometragem, 3) as distancia_total_planejada,
                -- Preserva os tipos do planejamento, inclusive quando são nulos.
                array_agg(
                    struct(data as data_operacional, tipo_dia, tipo_os)
                ) as dias_planejados
            from {{ ref("servico_planejado_faixa_horaria") }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                and sistema = 'SPPO'  -- Não compara com o sistema do RIO
            group by all
        ),
        data_versao_efetiva as (
            select data, feed_start_date, tipo_os, tipo_dia
            from {{ ref("subsidio_data_versao_efetiva") }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")

            union all

            select data, feed_start_date, tipo_os, tipo_dia
            from {{ ref("calendario") }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
        ),
        os as (
            select
                data,
                servico,
                consorcio,
                left(sentido, 1) as sentido,
                tipo_os,
                tipo_dia,
                faixa_horaria_inicio,
                partidas,
                quilometragem
            from data_versao_efetiva
            left join
                {{ ref("ordem_servico_faixa_horaria_sentido") }} using (
                    feed_start_date, tipo_os, tipo_dia
                )
            where
                data between greatest(
                    date("{{ var('date_range_start') }}"),
                    date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
                ) and date("{{ var('date_range_end') }}")
            full outer union all by name
            select
                data,
                servico,
                consorcio,
                tipo_os,
                tipo_dia,
                faixa_horaria_inicio,
                partidas,
                quilometragem
            from data_versao_efetiva
            left join
                {{ ref("ordem_servico_faixa_horaria") }} using (
                    feed_start_date, tipo_os, tipo_dia
                )
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                and data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
        ),
        os_faixa as (
            select
                date(
                    datetime(data) + interval cast(
                        split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
                    ) hour
                ) as data,
                servico,
                sentido,
                datetime(data) + interval cast(
                    split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
                ) hour as faixa_horaria_inicio,
                round(sum(quilometragem), 3) as quilometragem,
                -- Total de partidas previstas na faixa pela OS.
                sum(partidas) as partidas_os,
                -- Guarda a data e a classificação do calendário antes da virada do dia.
                array_agg(
                    struct(os.data as data_operacional, tipo_dia, tipo_os)
                ) as dias_os
            from os
            -- Restringe a OS aos consórcios do SPPO antes de somar as faixas.
            where
                lower(trim(consorcio))
                in ('internorte', 'intersul', 'santa cruz', 'transcarioca')
            group by all
        )
        {% if "viagem_planejada" not in model and "servico_planejado_faixa_horaria" not in model %}
            ,
            sumario as (
                select
                    data,
                    servico,
                    faixa_horaria_inicio,
                    km_planejada_faixa,
                    {% if var("date_range_end") >= var(
                        "DATA_SUBSIDIO_V17_INICIO"
                    ) and "pagamento" in model %}
                        sentido,
                    {% else %} cast(null as string) as sentido
                    {% endif %}
                from {{ model }}
                -- `rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento`
                where
                    data between date("{{ var('date_range_start') }}") and date(
                        "{{ var('date_range_end') }}"
                    )
            )
        {% endif %}
    select * except (partidas_os, dias_os, dias_planejados)
    from viagem_planejada p
    full join
        (
            select
                * except (servico),
                -- alteração referente ao Processo.rio MTR-CAP-2025/06098
                case
                    when faixa_horaria_inicio = "2025-06-01T00:00:00"
                    then
                        case
                            when servico = "LECD108"
                            then "LECD112"
                            when servico = "864"
                            then "LECD122"
                            else servico
                        end
                    else servico
                end as servico,
            from os_faixa
            where quilometragem != 0
        ) using (data, servico, faixa_horaria_inicio, sentido)
    {% if "viagem_planejada" not in model and "servico_planejado_faixa_horaria" not in model %}
        full join sumario using (data, servico, faixa_horaria_inicio, sentido)
    {% endif %}
    where
        (
            -- Detecta distância planejada diferente ou nula.
            quilometragem is distinct from distancia_total_planejada
            and (
                -- Sinaliza a faixa ausente com partidas previstas mesmo nas exceções.
                (
                    distancia_total_planejada is null
                    and partidas_os > 0
                    and p.data is null
                )
                -- Exceções adaptadas de test_consistencia_planejamento_feed.sql
                -- para a data operacional. Uma data fora delas gera alerta.
                or exists (
                    select 1
                    -- Usa o calendário para faixas ausentes ou do período histórico.
                    from unnest(coalesce(p.dias_planejados, dias_os)) as dia_os
                    where
                        not coalesce(
                            -- Dias de eleição, conforme o PR #818.
                            dia_os.data_operacional
                            in (date '2026-10-04', date '2026-10-25')
                            -- Ponto Facultativo a partir de 19/10, a pedido de
                            -- Matheus Dias Magalhães
                            -- no grupo Domínio de Planejamento em 09/11/2026, às 17h55.
                            or (
                                dia_os.data_operacional >= date '2026-10-19'
                                and dia_os.tipo_dia = 'Ponto Facultativo'
                            )
                            -- OS de Verão a partir de 14/11, a pedido de Matheus Dias
                            -- Magalhães
                            -- no grupo Domínio de Planejamento em 16/11/2026, às 19h43.
                            or (
                                dia_os.data_operacional >= date '2026-11-14'
                                and dia_os.tipo_os = 'Verão'
                            ),
                            false
                        )
                )
            )
        )
        {% if "viagem_planejada" not in model and "servico_planejado_faixa_horaria" not in model %}
            -- Compara também a distância planejada com a quilometragem do sumário.
            or distancia_total_planejada != km_planejada_faixa
        {% endif %}
{%- endtest %}
