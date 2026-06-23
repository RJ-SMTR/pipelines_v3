# -*- coding: utf-8 -*-
"""
Valores constantes para captura do calendário manual (datas atípicas)
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

CALENDARIO_MANUAL_SOURCE_NAME = "smtr"
CALENDARIO_MANUAL_TABLE_ID = "calendario_manual"

CALENDARIO_MANUAL_SHEET_ID = "1Jn7fmaDOhuHMdMqHo5SGWHCRuerXNWJRmhRjnHxJ9O4"
CALENDARIO_MANUAL_SHEET_NAME = "Dias Atípicos"
CALENDARIO_MANUAL_COLUMNS = [
    "dia",
    "tipo_dia",
    "subtipo_dia",
    "tipo_os",
    "despacho_observacao",
]

CALENDARIO_MANUAL_CUTOVER_DATE = "2026-07-01"

CALENDARIO_MANUAL_SOURCE = SourceTable(
    source_name=CALENDARIO_MANUAL_SOURCE_NAME,
    table_id=CALENDARIO_MANUAL_TABLE_ID,
    first_timestamp=datetime(2026, 7, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__calendario_manual",
    partition_date_only=True,
    max_recaptures=5,
    primary_keys=["dia"],
    raw_filetype="csv",
)
