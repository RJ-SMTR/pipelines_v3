# -*- coding: utf-8 -*-
"""Regras versionadas de remuneracao do subsidio SPPO."""

from openfisca_smtr.apurar import apurar, period_from_row, period_from_value
from openfisca_smtr.simulation import build_simulation_from_rows, calculate_columns, simulate
from openfisca_smtr.system import SubsidioRemuneracaoTaxBenefitSystem
from openfisca_smtr.version import get_versao_regra

__all__ = [
    "SubsidioRemuneracaoTaxBenefitSystem",
    "apurar",
    "build_simulation_from_rows",
    "calculate_columns",
    "get_versao_regra",
    "period_from_row",
    "period_from_value",
    "simulate",
]
