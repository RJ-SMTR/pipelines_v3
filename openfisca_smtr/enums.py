# -*- coding: utf-8 -*-
"""Enumeracoes normativas usadas nas regras de remuneracao."""

from openfisca_core.model_api import Enum


class Lote(Enum):
    """Lotes com parametros I.9 vigentes (B1 deserto — fora do enum).

    A0: lote de simulação (espelho A2) para testes com vigência jul/2026.
    """

    A0 = "A0"
    A2 = "A2"
    B2 = "B2"
