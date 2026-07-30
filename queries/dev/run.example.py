# -*- coding: utf-8 -*-
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries.dev.utils import fetch_dataset_sha, run_dbt_selector

# Copy this file to run.py and edit locally:
#   cp run.example.py run.py
#
# See available parameters in utils.py (run_dbt_model, run_dbt_selector, etc.)

run_dbt_selector(
    selector_name="<selector_name>",
    flags="--target dev --defer --state target-base --favor-state",
    _vars=[
        {
            "date_range_start": "YYYY-MM-DD",
            "date_range_end": "YYYY-MM-DD",
        },
        fetch_dataset_sha(),
    ],
)

# run_dbt_model(
#     model="<dataset>.<table>",
#     flags="--target dev --defer --state target-base --favor-state",
#     _vars={
#         "date_range_start": "YYYY-MM-DD",
#         "date_range_end": "YYYY-MM-DD",
#     },
# )
