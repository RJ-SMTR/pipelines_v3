# -*- coding: utf-8 -*-
"""API de simulacao OpenFisca independente de dbt/Spark.

Uso tipico (Python puro)::

    from openfisca_smtr.simulation import simulate

    resultado = simulate(
        rows=[
            {
                "id_apuracao": "v1",
                "indicador_viagem_completa": True,
                "hora_partida": 7,
                "day_of_week": 2,
            }
        ],
        period="2026-08-14",
        outputs=[
            "indicador_completa_pico_manha",
            "indicador_dia_util",
        ],
    )
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from openfisca_core.simulation_builder import SimulationBuilder
from openfisca_core.simulations import Simulation

from openfisca_smtr.system import SubsidioRemuneracaoTaxBenefitSystem

Periodo = str
LinhaFato = Mapping[str, Any]
LinhaResultado = dict[str, Any]


def build_simulation_from_rows(
    rows: Sequence[LinhaFato],
    *,
    period: Periodo,
    id_key: str = "id_apuracao",
    system: SubsidioRemuneracaoTaxBenefitSystem | None = None,
) -> Simulation:
    """Monta uma ``Simulation`` com uma entidade ``apuracao`` por linha.

    Cada ``row`` deve conter ``id_key`` e os nomes das variaveis OpenFisca
    a preencher (valores escalares para o ``period`` informado).
    """
    apuracoes: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        entity_id = str(row[id_key])
        payload: dict[str, dict[str, Any]] = {}
        for key, value in row.items():
            if key == id_key:
                continue
            payload[key] = {period: value}
        apuracoes[entity_id] = payload

    tax_benefit_system = system or SubsidioRemuneracaoTaxBenefitSystem()
    return SimulationBuilder().build_from_dict(
        tax_benefit_system,
        {"apuracoes": apuracoes},
    )


def calculate_columns(
    simulation: Simulation,
    variable_names: Sequence[str],
    period: Periodo,
) -> dict[str, list[Any]]:
    """Calcula variaveis e devolve listas alinhadas a ordem das entidades."""
    return {name: simulation.calculate(name, period).tolist() for name in variable_names}


def simulate(  # noqa: PLR0913
    rows: Sequence[LinhaFato],
    *,
    period: Periodo,
    outputs: Sequence[str],
    id_key: str = "id_apuracao",
    system: SubsidioRemuneracaoTaxBenefitSystem | None = None,
    keep_inputs: bool = True,
) -> list[LinhaResultado]:
    """Executa a simulacao e devolve uma lista de dicts (sem dbt/Spark).

    Parameters
    ----------
    rows:
        Fatos de entrada (uma linha = uma ``apuracao``).
    period:
        Periodo OpenFisca diario (ex.: ``\"2026-08-14\"``).
    outputs:
        Nomes das variaveis a calcular.
    id_key:
        Chave de identificacao da entidade em cada linha.
    system:
        TaxBenefitSystem opcional (reusa instancia se ja carregada).
    keep_inputs:
        Se True, ecoa as colunas de entrada em cada resultado.
    """
    if not rows:
        return []

    simulation = build_simulation_from_rows(
        rows,
        period=period,
        id_key=id_key,
        system=system,
    )
    calculado = calculate_columns(simulation, outputs, period)

    resultados: list[LinhaResultado] = []
    for indice, row in enumerate(rows):
        linha: LinhaResultado = {}
        if keep_inputs:
            linha.update(dict(row))
        else:
            linha[id_key] = row[id_key]
        for name in outputs:
            linha[name] = calculado[name][indice]
        resultados.append(linha)
    return resultados
