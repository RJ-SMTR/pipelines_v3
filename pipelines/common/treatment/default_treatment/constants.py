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
DBT_TEST_RESULT_ICONS = {"PASS": ":white_check_mark:", "WARN": ":warning:"}
DBT_TEST_STATUS_CIRCLES = {
    "FAIL": ":red_circle: ",
    "WARN": ":yellow_circle: ",
    "PASS": ":green_circle: ",
}
DBT_TEST_STATUS_MESSAGES = {
    "FAIL": ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n",
    "WARN": ":warning: **Status:** Sucesso com avisos. Verificar testes em WARN.",
    "PASS": ":tada: **Status:** Sucesso",
}
REPO_URL = "https://api.github.com/repos/RJ-SMTR/pipelines_v3"
DBT_STATE_BUCKET = "rj-smtr-public"
DBT_STATE_BLOB = "dbt-docs/manifest.json"
