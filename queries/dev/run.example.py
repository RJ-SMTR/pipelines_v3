# -*- coding: utf-8 -*-
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries.dev.utils import run_dbt_selector

# Copy this file to run.py and edit locally:
#   cp run.example.py run.py
#
# Seeds I.2 (uma vez / quando CSV mudar) — inclui stub A0 (jul/2026):
#   dbt seed --select sistema_referencia_servico sistema_referencia_lote \
#     sistema_referencia_lote_tecnologia --target dev --profiles-dir ./dev
#
# Cadeia de classificação (WIP: base viagem_completa) — 1ª quinzena jul/2026
# Lote A0 cobre 2026-07-01 … 2026-07-31 (452 serviços).

run_dbt_selector(
    selector_name=("remuneracao_openfisca"),
    flags="--target dev --defer --state target-base -x --profiles-dir ./dev ",
    _vars={
        "date_range_start": "2026-07-01",
        "date_range_end": "2026-07-15",
    },
)

# Pós-classificação (OF + FCF tipológico) — descomente quando openfisca_smtr
# estiver no ambiente dbt Python:
# run_dbt_model(
#     model="viagens_apuradas fcf_quinzena_lote remuneracao_quinzena_lote",
#     flags="--target dev --defer --state target-base --favor-state",
#     _vars={
#         "date_range_start": "2026-07-01",
#         "date_range_end": "2026-07-15",
#     },
# )
