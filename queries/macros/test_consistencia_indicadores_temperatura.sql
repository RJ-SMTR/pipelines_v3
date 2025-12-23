{% test test_consistencia_indicadores_temperatura(model) -%}
    {% set incremental_filter %}
    data between date("{{var('start_date')}}") and date("{{ var('end_date') }}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    {% endset %}

    with

        validador as (
            select data, id_viagem, id_validador
            from {{ ref("aux_viagem_temperatura") }}
            where {{ incremental_filter }}
        ),
        indicadores as (
            select
                data,
                ano_fabricacao,
                id_viagem,
                id_veiculo,
                safe_cast(
                    json_value(validador, '$.id_validador') as string
                ) as id_validador,
                safe_cast(
                    json_value(validador, '$.quantidade_pre_tratamento') as numeric
                ) as quantidade_pre_tratamento,
                safe_cast(
                    json_value(validador, '$.quantidade_nula') as numeric
                ) as quantidade_nula,
                safe_cast(
                    json_value(validador, '$.quantidade_zero') as numeric
                ) as quantidade_zero,
                safe_cast(
                    json_value(validador, '$.quantidade_pos_tratamento') as numeric
                ) as quantidade_pos_tratamento,
                safe_cast(
                    json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
                ) as indicador_ar_condicionado,
                safe_cast(
                    json_value(
                        indicadores,
                        '$.indicador_regularidade_ar_condicionado_viagem.valor'
                    ) as bool
                ) as indicador_regularidade_ar_condicionado_viagem,
                safe_cast(
                    json_value(
                        validador, '$.indicador_temperatura_transmitida_viagem'
                    ) as bool
                ) as indicador_temperatura_transmitida_viagem,
                safe_cast(
                    json_value(
                        validador, '$.indicador_temperatura_variacao_viagem'
                    ) as bool
                ) as indicador_temperatura_variacao_viagem,
                safe_cast(
                    json_value(validador, '$.indicador_temperatura_zero_viagem') as bool
                ) as indicador_temperatura_zero_viagem,
                safe_cast(
                    json_value(
                        validador,
                        '$.indicador_temperatura_pos_tratamento_descartada_viagem'
                    ) as bool
                ) as indicador_temperatura_pos_tratamento_descartada_viagem,
                safe_cast(
                    json_value(validador, '$.indicador_temperatura_nula_viagem') as bool
                ) as indicador_temperatura_nula_viagem,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_falha_recorrente.valor'
                    ) as bool
                ) as indicador_falha_recorrente
            from {{ model }}
            left join
                unnest(
                    json_query_array(indicadores, '$.indicador_validador.valores')
                ) as validador
            where {{ incremental_filter }}
        ),

        percentuais as (
            select
                data,
                id_viagem,
                id_validador,
                trunc(
                    (
                        1 - coalesce(
                            safe_divide(
                                quantidade_pos_tratamento, quantidade_pre_tratamento
                            ),
                            0
                        )
                    )
                    * 100,
                    2
                ) as test_percentual_temperatura_pos_tratamento_descartada,
                trunc(
                    coalesce(safe_divide(quantidade_zero, quantidade_pre_tratamento), 0)
                    * 100,
                    2
                ) as test_percentual_temperatura_zero_descartada,
                trunc(
                    coalesce(safe_divide(quantidade_nula, quantidade_pre_tratamento), 0)
                    * 100,
                    2
                ) as test_percentual_temperatura_nula_descartada
            from indicadores
        ),
        indicador_temperatura_transmitida_viagem as (
            select
                *,
                case
                    when
                        quantidade_pre_tratamento = 0
                        and indicador_temperatura_transmitida_viagem = true
                    then
                        'Nenhuma temperatura foi transmitida no indicador_temperatura_transmitida_viagem'
                end as temperatura_nao_transmitida,

                case
                    when
                        quantidade_pre_tratamento = quantidade_zero
                        and indicador_temperatura_transmitida_viagem = true
                    then
                        'Todas temperaturas transmitidas foram iguais a zero no indicador_temperatura_transmitida_viagem'
                end as todas_temperaturas_transmitidas_iguais_zero
            from indicadores
        ),

        indicador_temperatura_variacao_viagem as (
            select
                *,
                case
                    when
                        quantidade_pre_tratamento = quantidade_zero
                        and indicador_temperatura_variacao_viagem = true
                    then
                        'Todas temperaturas iguais a zero no indicador_temperatura_variacao_viagem'
                end as todas_temperaturas_zero
            from indicadores

        ),

        indicador_temperatura_pos_tratamento_descartada_viagem as (
            select
                *,
                case
                    when
                        p.test_percentual_temperatura_pos_tratamento_descartada < 50
                        and i.indicador_temperatura_pos_tratamento_descartada_viagem
                        = true
                    then
                        'Falha na temperatura descartada pós tratamento no indicador_temperatura_pos_tratamento_descartada_viagem'
                end as inconsistencia_descarte_pos
            from indicadores i
            left join percentuais p using (data, id_viagem, id_validador)
        ),

        indicador_temperatura_zero_descartada as (
            select
                *,
                case
                    when
                        p.test_percentual_temperatura_zero_descartada < 100
                        and i.indicador_temperatura_zero_viagem = true
                    then
                        'Falha na temperatura zero descartada no indicador_temperatura_zero_viagem'
                end as inconsistencia_temperatura_zero
            from indicadores i
            left join percentuais p using (data, id_viagem, id_validador)
        ),

        condicao_veiculo as (
            select
                *,
                case
                    when
                        (
                            (
                                i.ano_fabricacao <= 2019
                                or data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
                            )
                            and indicador_ar_condicionado
                            and p.test_percentual_temperatura_nula_descartada = 100
                            and i.indicador_regularidade_ar_condicionado_viagem
                            is not null
                        )
                    then
                        'Indicador_regularidade_ar_condicionado_viagem deveria ser nulo, 100% das temperaturas foram nulas'
                end as inconsistencia_temperatura_nula,
            from indicadores i
            left join percentuais p using (data, id_viagem, id_validador)
            left join validador v using (data, id_viagem, id_validador)
            where i.id_validador = v.id_validador
        ),

        falha_recorrente as (
            select
                v.data,
                v.id_veiculo,
                v.ano_fabricacao,
                v.quantidade_dia_falha_operacional,
                i.indicador_falha_recorrente,
                i.id_viagem
            from {{ ref("veiculo_regularidade_temperatura_dia") }} v
            left join indicadores i using (data, id_veiculo)
            where {{ incremental_filter }}
        ),

        analise_falha_recorrente as (
            select
                data,
                id_viagem,
                id_veiculo,
                ano_fabricacao,
                quantidade_dia_falha_operacional,
                indicador_falha_recorrente,
                case
                    when
                        quantidade_dia_falha_operacional < 6
                        and indicador_falha_recorrente = true
                    then 'Falha recorrente incorreta'
                end as falha_recorrente_inconsistencia
            from falha_recorrente
        )
    select
        i.data,
        i.id_viagem,
        i.id_validador,
        tt.temperatura_nao_transmitida,
        tt.todas_temperaturas_transmitidas_iguais_zero,
        vv.todas_temperaturas_zero,
        dd.inconsistencia_descarte_pos,
        iz.inconsistencia_temperatura_zero,
        cv.inconsistencia_temperatura_nula,
        fr.falha_recorrente_inconsistencia
    from indicadores i
    left join
        indicador_temperatura_transmitida_viagem tt using (
            data, id_viagem, id_validador
        )
    left join
        indicador_temperatura_variacao_viagem vv using (data, id_viagem, id_validador)
    left join
        indicador_temperatura_pos_tratamento_descartada_viagem dd using (
            data, id_viagem, id_validador
        )
    left join
        indicador_temperatura_zero_descartada iz using (data, id_viagem, id_validador)
    left join condicao_veiculo cv using (data, id_viagem, id_validador)
    left join analise_falha_recorrente fr using (data, id_viagem)
    where
        tt.temperatura_nao_transmitida is not null
        or tt.todas_temperaturas_transmitidas_iguais_zero is not null
        or vv.todas_temperaturas_zero is not null
        or dd.inconsistencia_descarte_pos is not null
        or iz.inconsistencia_temperatura_zero is not null
        or cv.inconsistencia_temperatura_nula is not null
        or fr.falha_recorrente_inconsistencia is not null
{%- endtest %}
