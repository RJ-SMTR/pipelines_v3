# -*- coding: utf-8 -*-
"""Variaveis de impacto por tipo de viagem (Anexo I.8 Tabela 2)."""

from openfisca_core.model_api import DAY, Variable, where

from openfisca_smtr.entities import apuracao


class indicador_viagem_completa(Variable):
    value_type = bool
    default_value = False
    entity = apuracao
    definition_period = DAY
    label = "Viagem completa (cumpriu o plano de km)"


class indicador_viagem_valida(Variable):
    value_type = bool
    default_value = False
    entity = apuracao
    definition_period = DAY
    label = "Validacao valida (conforme ou nao conforme; false = invalida)"


class indicador_viagem_conforme(Variable):
    value_type = bool
    default_value = False
    entity = apuracao
    definition_period = DAY
    label = "Validacao conforme (exige indicador_viagem_valida)"


class km_programada(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = DAY
    label = "Quilometragem programada da viagem (planejamento; usada se completa)"


class km_percorrida(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = DAY
    label = "Quilometragem percorrida reconhecida para viagem incompleta"


def _impacto_tab2(apuracoes, period, parameters, atributo: str):
    """Resolve YAML Tab. 2 a partir dos bools (completa / valida / conforme)."""
    completa = apuracoes("indicador_viagem_completa", period)
    valida = apuracoes("indicador_viagem_valida", period)
    conforme = apuracoes("indicador_viagem_conforme", period)
    impacto = parameters(period).impacto_tipo_viagem

    ramo_conforme = valida * conforme
    ramo_nao_conforme = valida * (1 - conforme)
    ramo_invalida = 1 - valida
    incompleta = 1 - completa

    completa_params = impacto.completa
    incompleta_params = impacto.incompleta

    return completa * (
        ramo_conforme * getattr(completa_params.conforme, atributo)
        + ramo_nao_conforme * getattr(completa_params.nao_conforme, atributo)
        + ramo_invalida * getattr(completa_params.invalida, atributo)
    ) + incompleta * (
        ramo_conforme * getattr(incompleta_params.conforme, atributo)
        + ramo_nao_conforme * getattr(incompleta_params.nao_conforme, atributo)
        + ramo_invalida * getattr(incompleta_params.invalida, atributo)
    )


class indicador_quilometragem_pagamento(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Indica se a viagem conta para quilometragem de pagamento"

    def formula(apuracoes, period, parameters):
        return _impacto_tab2(apuracoes, period, parameters, "quilometragem_pagamento")


class indicador_percentual_atendimento(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Indica se a viagem conta para percentual de atendimento"

    def formula(apuracoes, period, parameters):
        return _impacto_tab2(apuracoes, period, parameters, "percentual_atendimento")


class km_remuneravel(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Quilometragem remuneravel da viagem"

    def formula(apuracoes, period, parameters):
        completa = apuracoes("indicador_viagem_completa", period)
        indicador_pagamento = apuracoes("indicador_quilometragem_pagamento", period)
        km_completa = apuracoes("km_programada", period)
        km_incompleta = apuracoes("km_percorrida", period)
        km_base = where(completa, km_completa, km_incompleta)
        return where(indicador_pagamento, km_base, 0.0)


VARIABLES = [
    indicador_viagem_completa,
    indicador_viagem_valida,
    indicador_viagem_conforme,
    km_programada,
    km_percorrida,
    indicador_quilometragem_pagamento,
    indicador_percentual_atendimento,
    km_remuneravel,
]
