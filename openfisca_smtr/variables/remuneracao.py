# -*- coding: utf-8 -*-
"""Variaveis de frota diaria, OPEX por viagem e parametros I.9."""

from openfisca_core.model_api import DAY, ETERNITY, Enum, Variable

from openfisca_smtr.entities import apuracao
from openfisca_smtr.enums import Lote


class lote(Variable):
    value_type = Enum
    possible_values = Lote
    default_value = Lote.B2
    entity = apuracao
    definition_period = ETERNITY
    label = "Lote da apuracao (chave de parametros I.9)"


class lote_frota_estimada(Variable):
    """Fato I.2 opcional (eco). FCF contratual e quinzenal no dbt."""

    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = DAY
    label = "Frota estimada do lote (fato I.2; FCF quinzena no dbt)"


class frota_operante(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = DAY
    label = "Frota operante do lote x data (MAX picos; 0 se nao dia util; preenchida pelo apurar)"


class frota_pico_manha(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = DAY
    label = "Count distinct veiculos completa+pico manha (lote x data)"


class frota_pico_tarde(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = DAY
    label = "Count distinct veiculos completa+pico tarde (lote x data)"


class tarifa_remuneracao(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Tarifa de remuneracao do lote (parametro I.9)"

    def formula(apuracoes, period, parameters):
        valor_lote = apuracoes("lote", period)
        p = parameters(period).parametros_lote
        return (
            (valor_lote == Lote.A0) * p.A0.tarifa_remuneracao
            + (valor_lote == Lote.A2) * p.A2.tarifa_remuneracao
            + (valor_lote == Lote.B2) * p.B2.tarifa_remuneracao
        )


class alpha(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Percentual CAPEX da tarifa de remuneracao (parametro I.9)"

    def formula(apuracoes, period, parameters):
        valor_lote = apuracoes("lote", period)
        p = parameters(period).parametros_lote
        return (
            (valor_lote == Lote.A0) * p.A0.alpha
            + (valor_lote == Lote.A2) * p.A2.alpha
            + (valor_lote == Lote.B2) * p.B2.alpha
        )


class beta(Variable):
    value_type = float
    entity = apuracao
    definition_period = DAY
    label = "Percentual OPEX da tarifa de remuneracao (parametro I.9)"

    def formula(apuracoes, period, parameters):
        valor_lote = apuracoes("lote", period)
        p = parameters(period).parametros_lote
        return (
            (valor_lote == Lote.A0) * p.A0.beta
            + (valor_lote == Lote.A2) * p.A2.beta
            + (valor_lote == Lote.B2) * p.B2.beta
        )


VARIABLES = [
    lote,
    lote_frota_estimada,
    frota_operante,
    frota_pico_manha,
    frota_pico_tarde,
    tarifa_remuneracao,
    alpha,
    beta,
]
