# -*- coding: utf-8 -*-
import json
from pathlib import Path

from openfisca_smtr import simulate
from openfisca_smtr.__main__ import main


def test_simulate_standalone_api():
    resultado = simulate(
        rows=[
            {
                "id_apuracao": "v1",
                "indicador_viagem_completa": True,
                "hora_partida": 7,
                "day_of_week": 2,
            },
            {
                "id_apuracao": "v2",
                "indicador_viagem_completa": True,
                "hora_partida": 12,
                "day_of_week": 7,
            },
        ],
        period="2026-08-01",
        outputs=["indicador_completa_pico_manha", "indicador_dia_util"],
    )

    assert resultado[0]["indicador_completa_pico_manha"] is True
    assert resultado[0]["indicador_dia_util"] is True
    assert resultado[1]["indicador_completa_pico_manha"] is False
    assert resultado[1]["indicador_dia_util"] is False


def test_cli_simulate_from_file(tmp_path: Path, capsys):
    entrada = tmp_path / "fatos.json"
    entrada.write_text(
        json.dumps(
            [
                {
                    "id_apuracao": "v1",
                    "indicador_viagem_completa": True,
                    "hora_partida": 7,
                    "day_of_week": 2,
                }
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "simulate",
            "--period",
            "2026-08-01",
            "--output",
            "indicador_completa_pico_manha",
            "--input",
            str(entrada),
        ]
    )

    assert exit_code == 0
    saida = json.loads(capsys.readouterr().out)
    assert saida[0]["id_apuracao"] == "v1"
    assert saida[0]["indicador_completa_pico_manha"] is True
