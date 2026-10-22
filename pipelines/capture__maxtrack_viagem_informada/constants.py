# -*- coding: utf-8 -*-
"""Constantes do pipeline de captura Maxtrack viagem_informada."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

MAXTRACK_SOURCE_NAME = "maxtrack"
MAXTRACK_SECRET_PATH = "maxtrack_api"
VIAGEM_INFORMADA_BASE_URL = "https://api-sspo-rj.maxtrack.com.br/api/v1/viagens"
VIAGEM_INFORMADA_TABLE_ID = "viagem_informada"

VIAGEM_INFORMADA_SOURCE = SourceTable(
    source_name=MAXTRACK_SOURCE_NAME,
    table_id=VIAGEM_INFORMADA_TABLE_ID,
    first_timestamp=datetime(2026, 8, 22, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__maxtrack_viagem_informada",
    partition_date_only=True,
    max_recaptures=5,
    primary_keys=["id_viagem"],
)
