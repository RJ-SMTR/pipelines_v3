# -*- coding: utf-8 -*-
"""Fonte de viagem_informada da Conecta para o Sistema RIO."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.gps.constants import CONECTA_SOURCE_NAME
from pipelines.common.utils.gcp.bigquery import SourceTable

CONECTA_SECRET_PATH = "conecta_sistema_rio_api"
VIAGEM_INFORMADA_BASE_URL = "https://apiproducao.ccomobility.com.br/api/v1/viagens"

VIAGEM_INFORMADA_SOURCE = SourceTable(
    source_name=CONECTA_SOURCE_NAME,
    table_id="viagem_informada_sistema_rio",
    first_timestamp=datetime(2026, 10, 15, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__conecta_sistema_rio_viagem_informada",
    primary_keys=["id_viagem"],
    partition_date_only=True,
    max_recaptures=5,
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
)
