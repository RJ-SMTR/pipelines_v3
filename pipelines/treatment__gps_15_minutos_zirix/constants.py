# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_15_minutos zirix
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__zirix_realocacao import constants as realocacao_constants
from pipelines.capture__zirix_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

GPS_15_MINUTOS_ZIRIX_SELECTOR = DBTSelector(
    name="gps_15_minutos",
    initial_datetime=datetime(2025, 5, 27, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_15_minutos_zirix",
    redis_key_suffix="zirix",
    data_sources=[
        registros_constants.ZIRIX_REGISTROS_SOURCE,
        realocacao_constants.ZIRIX_REALOCACAO_SOURCE,
    ],
)
