# -*- coding: utf-8 -*-
# ruff: noqa: ARG002, N801, N805
"""Modelo dbt Python mínimo para testar execução do OpenFisca no dbt."""

from openfisca_core.entities import Entity
from openfisca_core.model_api import MONTH, Variable, where
from openfisca_core.simulation_builder import SimulationBuilder
from openfisca_core.taxbenefitsystems import TaxBenefitSystem
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

PERIODO_TESTE = "2026-08"

apuracao = Entity(
    key="apuracao",
    plural="apuracoes",
    label="Apuracao",
    doc="Unidade sintética para testar OpenFisca no dbt Python.",
)


class km_programada(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Quilometragem programada"


class indicador_quilometragem_pagamento(Variable):
    value_type = bool
    default_value = False
    entity = apuracao
    definition_period = MONTH
    label = "Indica se a viagem conta para quilometragem de pagamento"


class km_remuneravel(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Quilometragem remunerável"

    def formula(apuracoes, period, parameters):
        indicador_pagamento = apuracoes("indicador_quilometragem_pagamento", period)
        km = apuracoes("km_programada", period)
        return where(indicador_pagamento, km, 0.0)


class TesteOpenFiscaTaxBenefitSystem(TaxBenefitSystem):
    def __init__(self) -> None:
        super().__init__([apuracao])
        self.add_variable(km_programada)
        self.add_variable(indicador_quilometragem_pagamento)
        self.add_variable(km_remuneravel)


def model(dbt, session):
    dbt.config(
        materialized="table",
        tags=["teste_openfisca"],
    )

    casos_teste = {
        "viagem_remuneravel": {
            "indicador_quilometragem_pagamento": {PERIODO_TESTE: True},
            "km_programada": {PERIODO_TESTE: 10.0},
        },
        "viagem_nao_remuneravel": {
            "indicador_quilometragem_pagamento": {PERIODO_TESTE: False},
            "km_programada": {PERIODO_TESTE: 10.0},
        },
    }

    sistema = TesteOpenFiscaTaxBenefitSystem()
    simulacao = SimulationBuilder().build_from_dict(
        sistema,
        {"apuracoes": casos_teste},
    )

    indicador_quilometragem = simulacao.calculate(
        "indicador_quilometragem_pagamento",
        PERIODO_TESTE,
    )
    km_remuneravel = simulacao.calculate("km_remuneravel", PERIODO_TESTE)

    linhas = [
        (
            id_apuracao,
            bool(indicador_quilometragem[indice]),
            float(km_remuneravel[indice]),
        )
        for indice, id_apuracao in enumerate(casos_teste)
    ]

    schema = StructType(
        [
            StructField("id_apuracao", StringType(), False),
            StructField("indicador_quilometragem_pagamento", BooleanType(), False),
            StructField("km_remuneravel", DoubleType(), False),
        ]
    )

    return session.createDataFrame(linhas, schema)
