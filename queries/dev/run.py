# -*- coding: utf-8 -*-
# import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from queries.dev.utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="example",
    table_id="my_first_dbt_model",
)
