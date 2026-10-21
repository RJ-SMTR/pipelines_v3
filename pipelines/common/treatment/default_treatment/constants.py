# -*- coding: utf-8 -*-
"""
Valores constantes flow de materialização genérico da rj-smtr
"""

REDIS_LAST_MATERIALIZATION_TS_KEY = "last_run_timestamp"
MATERIALIZATION_LAST_RUN_PATTERN = "%Y-%m-%dT%H:%M:%S"
# Variáveis do dbt que identificam o que foi testado, da mais específica para a mais genérica
DBT_VARS_INTERVAL_KEYS = (
    ("data_versao_gtfs", None),
    ("run_date", None),
    ("date_range_start", "date_range_end"),
    ("start_date", "end_date"),
)
REPO_URL = "https://api.github.com/repos/RJ-SMTR/pipelines_v3"
DBT_STATE_BUCKET = "rj-smtr-public"
DBT_STATE_BLOB = "dbt-docs/manifest.json"
