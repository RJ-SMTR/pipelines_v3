# -*- coding: utf-8 -*-
"""Valores contantes para captura dos dados das planilhas de controle_financeiro"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

SMTR_SOURCE_NAME = "smtr"

SHEETS_BASE_URL = "https://docs.google.com/spreadsheets/d/\
1QVfa9b8jzpQr3gac0FIlozmTaVeArtJROA343A2lMVM/export?format=csv&gid="

SHEETS_CAPTURE_PARAMS = {
    "cb": {
        "sheet_id": "454453523",
    },
    "cett": {
        "sheet_id": "0",
    },
}

CONTROLE_FINANCEIRO_SOURCES = [
    SourceTable(
        source_name=SMTR_SOURCE_NAME,
        table_id=t,
        first_timestamp=datetime(2026, 8, 20, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        partition_date_only=True,
        raw_filetype="csv",
        primary_keys=[],
    )
    for t in SHEETS_CAPTURE_PARAMS
]
