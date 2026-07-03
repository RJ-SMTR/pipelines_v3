# -*- coding: utf-8 -*-
"""Sistema OpenFisca da remuneracao do subsidio SPPO."""

from pathlib import Path

from openfisca_core.taxbenefitsystems import TaxBenefitSystem

from openfisca_smtr import variables
from openfisca_smtr.entities import apuracao

PARAMETERS_DIR = Path(__file__).parent / "parameters"


class SubsidioRemuneracaoTaxBenefitSystem(TaxBenefitSystem):
    """TaxBenefitSystem com as regras do Anexo I.8."""

    def __init__(self) -> None:
        super().__init__([apuracao])
        self.load_parameters(str(PARAMETERS_DIR))
        for variable in variables.VARIABLES:
            self.add_variable(variable)
