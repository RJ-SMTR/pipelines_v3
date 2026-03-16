# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de GPS realocacao da Cittati
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common.capture.gps import constants as gps_constants

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

CITTATI_REALOCACAO_SOURCE = SourceTable(
    source_name=gps_constants.CITTATI_SOURCE_NAME,
    table_id=gps_constants.REALOCACAO_TABLE_ID,
    first_timestamp=datetime(2025, 5, 9, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__cittati_realocacao",
    primary_keys=["id_veiculo", "datetime_processamento"],
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
)
