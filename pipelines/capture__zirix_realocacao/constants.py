# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de GPS realocacao da Zirix
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.gps import constants as gps_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

ZIRIX_REALOCACAO_SOURCE = SourceTable(
    source_name=gps_constants.ZIRIX_SOURCE_NAME,
    table_id=gps_constants.REALOCACAO_TABLE_ID,
    first_timestamp=datetime(2025, 5, 9, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__zirix_realocacao",
    primary_keys=["id_veiculo", "datetime_processamento"],
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
)
