# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados do GPS do validador da Jaé
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

GPS_VALIDADOR_SOURCE = SourceTable(
    source_name=jae_constants.JAE_SOURCE_NAME,
    table_id=jae_constants.GPS_VALIDADOR_TABLE_ID,
    first_timestamp=datetime(2025, 3, 26, 15, 30, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__jae_gps_validador",
    primary_keys=["id"],
)
