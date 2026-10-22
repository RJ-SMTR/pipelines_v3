# -*- coding: utf-8 -*-
"""Valores constantes para captura de dados de GPS da Maxtrack."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.gps import constants as gps_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

MAXTRACK_REGISTROS_SOURCE = SourceTable(
    source_name=gps_constants.MAXTRACK_SOURCE_NAME,
    table_id=gps_constants.REGISTROS_TABLE_ID,
    first_timestamp=datetime(2026, 8, 22, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__maxtrack_registros",
    primary_keys=["id_registro"],
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
)
