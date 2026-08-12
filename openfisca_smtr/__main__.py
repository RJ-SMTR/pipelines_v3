# -*- coding: utf-8 -*-
"""CLI do pacote openfisca_smtr (independente de dbt).

Exemplos::

    # API generica (simulate) — period diario obrigatorio
    python -m openfisca_smtr simulate --period 2026-08-14 \\
        --output indicador_completa_pico_manha --input fatos.json

    # Apuracao no grao viagem — period / hora / dow derivados de datetime_partida
    python -m openfisca_smtr apurar --viagens viagens.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, TextIO

from openfisca_smtr.apurar import apurar
from openfisca_smtr.simulation import simulate


def _load_json(path: Path | None, stdin: TextIO) -> Any:
    raw = path.read_text(encoding="utf-8") if path is not None else stdin.read()
    return json.loads(raw)


def _load_rows(path: Path | None, stdin: TextIO) -> list[dict[str, Any]]:
    data = _load_json(path, stdin)
    if isinstance(data, dict) and "rows" in data:
        rows = data["rows"]
    else:
        rows = data
    if not isinstance(rows, list):
        msg = 'JSON de entrada deve ser uma lista de objetos ou {"rows": [...]}.'
        raise SystemExit(msg)
    return rows


def _cmd_simulate(args: argparse.Namespace) -> int:
    rows = _load_rows(args.input, sys.stdin)
    resultados = simulate(
        rows,
        period=args.period,
        outputs=args.outputs,
        id_key=args.id_key,
    )
    json.dump(resultados, sys.stdout, ensure_ascii=False, indent=2, default=str)
    sys.stdout.write("\n")
    return 0


def _cmd_apurar(args: argparse.Namespace) -> int:
    viagens = _load_rows(args.viagens, sys.stdin) if args.viagens else []
    resultado = apurar(
        viagens=viagens or None,
        id_key=args.id_key,
        id_execucao=args.id_execucao,
    )
    json.dump(resultado, sys.stdout, ensure_ascii=False, indent=2, default=str)
    sys.stdout.write("\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m openfisca_smtr",
        description="OpenFisca SMTR — simulacao independente de dbt.",
    )
    sub = parser.add_subparsers(dest="comando", required=True)

    p_sim = sub.add_parser("simulate", help="Simulacao generica por lista de outputs.")
    p_sim.add_argument(
        "--period",
        required=True,
        help="Periodo OpenFisca diario (YYYY-MM-DD).",
    )
    p_sim.add_argument("--output", action="append", dest="outputs", required=True)
    p_sim.add_argument("--input", type=Path, default=None)
    p_sim.add_argument("--id-key", default="id_apuracao")
    p_sim.set_defaults(func=_cmd_simulate)

    p_ap = sub.add_parser("apurar", help="Apuracao no grao viagem.")
    p_ap.add_argument(
        "--viagens",
        type=Path,
        default=None,
        help="JSON de viagens (datetime_partida obrigatorio por row).",
    )
    p_ap.add_argument("--id-key", default="id_apuracao")
    p_ap.add_argument("--id-execucao", default=None)
    p_ap.set_defaults(func=_cmd_apurar)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
