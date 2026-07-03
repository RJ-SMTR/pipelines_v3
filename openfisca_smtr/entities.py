# -*- coding: utf-8 -*-
"""Entidades OpenFisca da remuneracao do subsidio SPPO."""

from openfisca_core.entities import Entity

apuracao = Entity(
    key="apuracao",
    plural="apuracoes",
    label="Apuracao",
    doc="Unidade de calculo das regras de remuneracao.",
)
