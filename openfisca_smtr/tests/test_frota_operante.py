# -*- coding: utf-8 -*-
from openfisca_core.simulation_builder import SimulationBuilder
from openfisca_smtr.system import SubsidioRemuneracaoTaxBenefitSystem


def test_openfisca_frota_pico_e_dia_util():
    system = SubsidioRemuneracaoTaxBenefitSystem()
    simulation = SimulationBuilder().build_from_dict(
        system,
        {
            "apuracoes": {
                "completa_manha": {
                    "indicador_viagem_completa": {"2026-08-01": True},
                    "hora_partida": {"2026-08-01": 7},
                    "day_of_week": {"2026-08-01": 2},
                },
                "completa_fora_pico": {
                    "indicador_viagem_completa": {"2026-08-01": True},
                    "hora_partida": {"2026-08-01": 12},
                    "day_of_week": {"2026-08-01": 2},
                },
                "incompleta_tarde": {
                    "indicador_viagem_completa": {"2026-08-01": False},
                    "hora_partida": {"2026-08-01": 17},
                    "day_of_week": {"2026-08-01": 7},
                },
                "completa_tarde_limite": {
                    "indicador_viagem_completa": {"2026-08-01": True},
                    "hora_partida": {"2026-08-01": 20},
                    "day_of_week": {"2026-08-01": 6},
                },
            },
        },
    )

    assert simulation.calculate("indicador_pico_manha", "2026-08-01").tolist() == [
        True,
        False,
        False,
        False,
    ]
    assert simulation.calculate("indicador_pico_tarde", "2026-08-01").tolist() == [
        False,
        False,
        True,
        False,
    ]
    assert simulation.calculate("indicador_completa_pico_manha", "2026-08-01").tolist() == [
        True,
        False,
        False,
        False,
    ]
    assert simulation.calculate("indicador_completa_pico_tarde", "2026-08-01").tolist() == [
        False,
        False,
        False,
        False,
    ]
    assert simulation.calculate("indicador_dia_util", "2026-08-01").tolist() == [
        True,
        True,
        False,
        True,
    ]
