# -*- coding: utf-8 -*-
from openfisca_core.simulation_builder import SimulationBuilder
from openfisca_smtr.system import SubsidioRemuneracaoTaxBenefitSystem


def test_openfisca_impacto_tipo_viagem():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "v1": {
                    "indicador_viagem_completa": {"2026-08-01": True},
                    "indicador_viagem_valida": {"2026-08-01": True},
                    "indicador_viagem_conforme": {"2026-08-01": True},
                    "km_programada": {"2026-08-01": 10.0},
                },
                "v2": {
                    "indicador_viagem_completa": {"2026-08-01": True},
                    "indicador_viagem_valida": {"2026-08-01": True},
                    "indicador_viagem_conforme": {"2026-08-01": False},
                    "km_programada": {"2026-08-01": 10.0},
                },
                "v3": {
                    "indicador_viagem_completa": {"2026-08-01": False},
                    "indicador_viagem_valida": {"2026-08-01": True},
                    "indicador_viagem_conforme": {"2026-08-01": True},
                    "km_programada": {"2026-08-01": 10.0},
                    "km_percorrida": {"2026-08-01": 4.0},
                },
                "v4": {
                    "indicador_viagem_completa": {"2026-08-01": True},
                    "indicador_viagem_valida": {"2026-08-01": False},
                    "indicador_viagem_conforme": {"2026-08-01": False},
                    "km_programada": {"2026-08-01": 10.0},
                },
            },
        },
    )

    assert simulation.calculate("indicador_quilometragem_pagamento", "2026-08-01").tolist() == [
        True,
        False,
        True,
        False,
    ]
    assert simulation.calculate("indicador_percentual_atendimento", "2026-08-01").tolist() == [
        True,
        True,
        True,
        False,
    ]
    assert simulation.calculate("km_remuneravel", "2026-08-01").tolist() == [10.0, 0.0, 4.0, 0.0]


def test_openfisca_parametros_lote():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "lote_b2": {
                    "lote": {"2026-07-01": "B2"},
                },
                "lote_a2": {
                    "lote": {"2026-07-01": "A2"},
                },
                "lote_a0": {
                    "lote": {"2026-07-01": "A0"},
                },
            },
        },
    )
    assert round(float(simulation.calculate("tarifa_remuneracao", "2026-07-01")[0]), 2) == 11.53
    assert round(float(simulation.calculate("alpha", "2026-07-01")[0]), 2) == 0.22
    assert round(float(simulation.calculate("beta", "2026-07-01")[0]), 2) == 0.78
    assert round(float(simulation.calculate("tarifa_remuneracao", "2026-07-01")[1]), 2) == 9.94
    assert round(float(simulation.calculate("alpha", "2026-07-01")[1]), 2) == 0.24
    assert round(float(simulation.calculate("beta", "2026-07-01")[1]), 2) == 0.76
    assert round(float(simulation.calculate("tarifa_remuneracao", "2026-07-01")[2]), 2) == 9.94
    assert round(float(simulation.calculate("alpha", "2026-07-01")[2]), 2) == 0.24
    assert round(float(simulation.calculate("beta", "2026-07-01")[2]), 2) == 0.76


def test_openfisca_ipa_e_desconto_operacao_precaria():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "integral": {
                    "viagens_atendimento": {"2026-08-01": 90},
                    "viagens_programadas": {"2026-08-01": 100},
                },
                "intermediario": {
                    "viagens_atendimento": {"2026-08-01": 80},
                    "viagens_programadas": {"2026-08-01": 100},
                },
                "minimo": {
                    "viagens_atendimento": {"2026-08-01": 60},
                    "viagens_programadas": {"2026-08-01": 100},
                },
                "moderado": {
                    "viagens_atendimento": {"2026-08-01": 59},
                    "viagens_programadas": {"2026-08-01": 100},
                },
                "grave": {
                    "viagens_atendimento": {"2026-08-01": 39},
                    "viagens_programadas": {"2026-08-01": 100},
                },
            },
        },
    )

    assert [round(float(value), 2) for value in simulation.calculate("ipa", "2026-08-01")] == [
        1.0,
        0.9,
        0.6,
        0.0,
        0.0,
    ]
    assert simulation.calculate("desconto_operacao_precaria", "2026-08-01").tolist() == [
        0.0,
        0.0,
        0.0,
        600.0,
        1200.0,
    ]
