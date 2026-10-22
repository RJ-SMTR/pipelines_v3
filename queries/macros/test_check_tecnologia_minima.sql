{% test test_check_tecnologia_minima(model) %}

    with
        viagem_classificada as (
            select
                data,
                id_viagem,
                id_veiculo,
                tipo_viagem,
                tecnologia_apurada,
                tecnologia_remunerada,
                safe_cast(
                    json_value(
                        indicadores, '$.indicador_penalidade_tecnologia.valor'
                    ) as bool
                ) as indicador_penalidade_tecnologia
            from {{ model }}
        ),
        validation as (
            select
                data,
                id_viagem,
                id_veiculo,
                tipo_viagem,
                tecnologia_apurada,
                tecnologia_remunerada,
                indicador_penalidade_tecnologia
            from viagem_classificada
            where
                indicador_penalidade_tecnologia
                and tipo_viagem not in (
                    "Não licenciado",
                    "Não vistoriado",
                    "Lacrado",
                    "Não autorizado por ausência de ar-condicionado",
                    "Não autorizado por capacidade"
                )
        )
    select *
    from validation

{% endtest %}
