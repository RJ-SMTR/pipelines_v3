# -*- coding: utf-8 -*-
"""Constantes do pipeline de captura Rio Ônibus"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

RIO_ONIBUS_SOURCE_NAME = "rioonibus"
RIO_ONIBUS_SECRET_PATH = "rioonibus_api"
VIAGEM_INFORMADA_BASE_URL = "https://us-east1-papo-tec.cloudfunctions.net/viagem_informada_smtr"
VIAGEM_INFORMADA_TABLE_ID = "viagem_informada"

FLOW_FOLDER_NAME = "capture__rioonibus_viagem_informada"

VIAGEM_INFORMADA_SOURCE = SourceTable(
    source_name=RIO_ONIBUS_SOURCE_NAME,
    table_id=VIAGEM_INFORMADA_TABLE_ID,
    first_timestamp=datetime(2024, 10, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name=FLOW_FOLDER_NAME,
    partition_date_only=True,
    max_recaptures=5,
    primary_keys=["id_viagem"],
)
