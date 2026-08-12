# -*- coding: utf-8 -*-
"""Variaveis de frota e pico (Anexo I.8 item 4)."""

from openfisca_core.model_api import DAY, Variable

from openfisca_smtr.entities import apuracao


class hora_partida(Variable):
    value_type = int
    default_value = -1
    entity = apuracao
    definition_period = DAY
    label = "Hora da partida da viagem (0-23)"


class day_of_week(Variable):
    value_type = int
    default_value = 0
    entity = apuracao
    definition_period = DAY
    label = "Dia da semana Spark (1=domingo ... 7=sabado)"


class indicador_pico_manha(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Partida no pico manha [hora_inicio, hora_fim)"

    def formula(apuracoes, period, parameters):
        hora = apuracoes("hora_partida", period)
        pico = parameters(period).frota.pico_manha
        return (hora >= pico.hora_inicio) * (hora < pico.hora_fim)


class indicador_pico_tarde(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Partida no pico tarde [hora_inicio, hora_fim)"

    def formula(apuracoes, period, parameters):
        hora = apuracoes("hora_partida", period)
        pico = parameters(period).frota.pico_tarde
        return (hora >= pico.hora_inicio) * (hora < pico.hora_fim)


class indicador_completa_pico_manha(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Viagem completa com partida no pico manha"

    def formula(apuracoes, period, parameters):
        completa = apuracoes("indicador_viagem_completa", period)
        pico = apuracoes("indicador_pico_manha", period)
        return completa * pico


class indicador_completa_pico_tarde(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Viagem completa com partida no pico tarde"

    def formula(apuracoes, period, parameters):
        completa = apuracoes("indicador_viagem_completa", period)
        pico = apuracoes("indicador_pico_tarde", period)
        return completa * pico


class indicador_dia_util(Variable):
    value_type = bool
    entity = apuracao
    definition_period = DAY
    label = "Data da viagem e dia util (segunda a sexta)"

    def formula(apuracoes, period, parameters):
        dow = apuracoes("day_of_week", period)
        dia_util = parameters(period).frota.dia_util
        return (dow >= dia_util.dayofweek_inicio) * (dow <= dia_util.dayofweek_fim)


VARIABLES = [
    hora_partida,
    day_of_week,
    indicador_pico_manha,
    indicador_pico_tarde,
    indicador_completa_pico_manha,
    indicador_completa_pico_tarde,
    indicador_dia_util,
]
