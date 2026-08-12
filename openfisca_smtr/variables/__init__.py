# -*- coding: utf-8 -*-
"""Registro de variaveis OpenFisca por dominio."""

from openfisca_smtr.variables import frota, impacto_viagem, ipa, remuneracao

VARIABLES = [
    *impacto_viagem.VARIABLES,
    *frota.VARIABLES,
    *ipa.VARIABLES,
    *remuneracao.VARIABLES,
]

__all__ = ["VARIABLES"]
