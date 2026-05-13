# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de temperatura do INMET

"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

INMET_SOURCE_NAME = "inmet"
INMET_SECRET_PATH = "inmet_api"
INMET_BASE_URL = "https://apitempo.inmet.gov.br/token/estacao"

INMET_ESTACOES = [
    "A602",
    "A621",
    "A636",
    "A651",
    "A652",
    "A653",
    "A654",
    "A655",
    "A656",
]

INMET_METEOROLOGIA_TABLE_ID = "meteorologia"
INMET_METEOROLOGIA_SOURCE = SourceTable(
    source_name=INMET_SOURCE_NAME,
    table_id=INMET_METEOROLOGIA_TABLE_ID,
    first_timestamp=datetime(2025, 7, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__inmet_temperatura",
    primary_keys=["CD_ESTACAO", "DT_MEDICAO", "HR_MEDICAO"],
    partition_date_only=True,
)

INMET_TEMPERATURA_SOURCES = [INMET_METEOROLOGIA_SOURCE]