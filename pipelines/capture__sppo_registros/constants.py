# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de GPS registros do SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.gps import constants as gps_constants
from pipelines.common.capture.gps.utils import rename_sppo_registros
from pipelines.common.utils.gcp.bigquery import SourceTable

SPPO_REGISTROS_SOURCE = SourceTable(
    source_name=gps_constants.SPPO_SOURCE_NAME,
    table_id=gps_constants.REGISTROS_TABLE_ID,
    first_timestamp=datetime(2026, 4, 16, 17, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__sppo_registros",
    primary_keys=["id_veiculo", "datetime_servidor"],
    pretreatment_reader_args={"dtype": "object", "convert_dates": False},
    pretreat_funcs=[rename_sppo_registros],
)
