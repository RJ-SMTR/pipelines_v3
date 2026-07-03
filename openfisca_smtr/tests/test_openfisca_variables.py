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
                    "tipo_apuracao": {"2026-08": "completa"},
                    "classificacao_validacao": {"2026-08": "conforme"},
                    "km_programada": {"2026-08": 10.0},
                },
                "v2": {
                    "tipo_apuracao": {"2026-08": "completa"},
                    "classificacao_validacao": {"2026-08": "nao_conforme"},
                    "km_programada": {"2026-08": 10.0},
                },
                "v3": {
                    "tipo_apuracao": {"2026-08": "incompleta"},
                    "classificacao_validacao": {"2026-08": "conforme"},
                    "km_programada": {"2026-08": 10.0},
                    "km_proporcional_incompleta": {"2026-08": 4.0},
                },
            },
        },
    )

    assert simulation.calculate("indicador_quilometragem_pagamento", "2026-08").tolist() == [
        True,
        False,
        True,
    ]
    assert simulation.calculate("indicador_percentual_atendimento", "2026-08").tolist() == [
        True,
        True,
        True,
    ]
    assert simulation.calculate("km_remuneravel", "2026-08").tolist() == [10.0, 0.0, 4.0]


def test_openfisca_ipa_e_desconto_operacao_precaria():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "integral": {
                    "viagens_atendimento": {"2026-08": 90},
                    "viagens_programadas": {"2026-08": 100},
                },
                "intermediario": {
                    "viagens_atendimento": {"2026-08": 80},
                    "viagens_programadas": {"2026-08": 100},
                },
                "minimo": {
                    "viagens_atendimento": {"2026-08": 60},
                    "viagens_programadas": {"2026-08": 100},
                },
                "moderado": {
                    "viagens_atendimento": {"2026-08": 59},
                    "viagens_programadas": {"2026-08": 100},
                },
                "grave": {
                    "viagens_atendimento": {"2026-08": 39},
                    "viagens_programadas": {"2026-08": 100},
                },
            },
        },
    )

    assert [round(float(value), 2) for value in simulation.calculate("ipa", "2026-08")] == [
        1.0,
        0.9,
        0.6,
        0.0,
        0.0,
    ]
    assert simulation.calculate("desconto_operacao_precaria", "2026-08").tolist() == [
        0.0,
        0.0,
        0.0,
        600.0,
        1200.0,
    ]


def test_openfisca_prd_faixas_idt():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "sem_reducao": {"idt": {"2026-08": 2.49}},
                "dois": {"idt": {"2026-08": 2.5}},
                "quatro": {"idt": {"2026-08": 7.5}},
                "seis": {"idt": {"2026-08": 12.5}},
                "oito": {"idt": {"2026-08": 17.5}},
            },
        },
    )

    assert [round(float(value), 2) for value in simulation.calculate("prd", "2026-08")] == [
        0.0,
        0.02,
        0.04,
        0.06,
        0.08,
    ]


def test_openfisca_remuneracao_lote_quinzena():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "lote_a": {
                    "tarifa_remuneracao": {"2026-08": 10.0},
                    "alpha": {"2026-08": 0.4},
                    "beta": {"2026-08": 0.6},
                    "km_referencia": {"2026-08": 100.0},
                    "fcf": {"2026-08": 1.0},
                    "km_ponderada_ipa": {"2026-08": 80.0},
                    "idt": {"2026-08": 3.0},
                    "receita_tarifa_publica": {"2026-08": 700.0},
                    "saldo_compensacao_anterior": {"2026-08": 20.0},
                }
            },
        },
    )

    assert round(float(simulation.calculate("remuneracao_servico", "2026-08")[0]), 2) == 870.4
    assert round(float(simulation.calculate("subsidio_bruto", "2026-08")[0]), 2) == 170.4
    assert round(float(simulation.calculate("subsidio_liquido", "2026-08")[0]), 2) == 150.4
    assert float(simulation.calculate("saldo_compensacao_posterior", "2026-08")[0]) == 0.0
