# -*- coding: utf-8 -*-
# import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries.dev.utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="transacao_erro",
    _vars={"date_range_start": "2026-03-05 00:00:00", "date_range_end": "2026-03-10 00:00:00"},
    flags="--defer --state target-base --favor-state",
)
