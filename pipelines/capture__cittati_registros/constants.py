# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de GPS registros da Cittati
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common.capture.gps import constants as gps_constants

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

CITTATI_REGISTROS_SOURCE = SourceTable(
    source_name=gps_constants.CITTATI_SOURCE_NAME,
    table_id=gps_constants.REGISTROS_TABLE_ID,
    first_timestamp=datetime(2025, 5, 9, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__cittati_registros",
    primary_keys=["id_veiculo", "datetime_servidor"],
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
)
