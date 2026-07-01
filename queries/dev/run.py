# -*- coding: utf-8 -*-
# import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries.dev.utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="staging_licenciamento_stu veiculo_licenciamento_dia",
    # upstream=True,
    _vars={"date_range_start": "2026-06-27", "date_range_end": "2026-06-27"},
    flags="--target dev --defer --state target-base --favor-state",
)

# run_dbt_model(
#     dataset_id="autuacao_disciplinar_historico staging_infracao",
#     # upstream=True,
#     _vars={"date_range_start": "2026-06-25", "date_range_end": "2026-06-25"},
#     flags="--target dev --defer --state target-base --favor-state --full-refresh",
# )