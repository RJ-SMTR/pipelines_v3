# -*- coding: utf-8 -*-
"""Variaveis de IPA e operacao precaria (Anexo I.8 itens 6.1 e 6.2)."""

from openfisca_core.model_api import DAY, Variable, where

from openfisca_smtr.entities import apuracao


class viagens_programadas(Variable):
    value_type = int
    default_value = 0
    entity = apuracao
    definition_period = DAY
    label = "Quantidade de viagens programadas no intervalo"


class viagens_atendimento(Variable):
    value_type = int
    default_value = 0
    entity = apuracao
    definition_period = DAY
    label = "Quantidade de viagens que contam para atendimento"


class percentual_atendimento(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Percentual de atendimento"

    def formula(apuracoes, period, parameters):
        atendimento = apuracoes("viagens_atendimento", period)
        programadas = apuracoes("viagens_programadas", period)
        return where(programadas > 0, atendimento / programadas, 0.0)


class ipa(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Indicador de percentual de atendimento"

    def formula(apuracoes, period, parameters):
        percentual = apuracoes("percentual_atendimento", period)
        ipa_params = parameters(period).ipa
        return where(
            percentual >= ipa_params.thresholds[3],
            ipa_params.amounts[3],
            where(
                percentual >= ipa_params.thresholds[2],
                ipa_params.amounts[2],
                where(
                    percentual >= ipa_params.thresholds[1],
                    ipa_params.amounts[1],
                    ipa_params.amounts[0],
                ),
            ),
        )


class desconto_operacao_precaria(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Desconto fixo por operacao precaria"

    def formula(apuracoes, period, parameters):
        percentual = apuracoes("percentual_atendimento", period)
        operacao_precaria = parameters(period).operacao_precaria
        return where(
            (percentual >= operacao_precaria.limite_grave)
            * (percentual < operacao_precaria.limite_moderado),
            operacao_precaria.desconto_moderado,
            where(
                percentual < operacao_precaria.limite_grave,
                operacao_precaria.desconto_grave,
                0.0,
            ),
        )


VARIABLES = [
    viagens_programadas,
    viagens_atendimento,
    percentual_atendimento,
    ipa,
    desconto_operacao_precaria,
]
