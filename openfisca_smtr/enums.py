# -*- coding: utf-8 -*-
"""Enumeracoes normativas usadas nas regras de remuneracao."""

from openfisca_core.model_api import Enum


class TipoApuracao(Enum):
    """Tipo de apuracao da viagem conforme Anexo I.8."""

    completa = "completa"
    incompleta = "incompleta"
    nao_apurada = "nao_apurada"


class ClassificacaoValidacao(Enum):
    """Classificacao de validacao da viagem conforme Anexo I.8."""

    conforme = "conforme"
    nao_conforme = "nao_conforme"
    invalida = "invalida"
