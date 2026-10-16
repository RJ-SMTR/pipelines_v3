# -*- coding: utf-8 -*-
"""Fonte de registros da Conecta para o Sistema RIO."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.gps.constants import CONECTA_SOURCE_NAME
from pipelines.common.utils.gcp.bigquery import SourceTable

REGISTROS_SOURCE = SourceTable(
    source_name=CONECTA_SOURCE_NAME,
    table_id="registros_sistema_rio",
    first_timestamp=datetime(2026, 10, 16, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__conecta_sistema_rio_registros",
    primary_keys=["id_registro"],
    partition_date_only=False,
    max_recaptures=60,
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
)
