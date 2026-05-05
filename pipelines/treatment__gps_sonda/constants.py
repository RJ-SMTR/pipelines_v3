# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps Sonda (BRT)
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__sonda_registros import constants as sonda_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

ADDITIONAL_VARS = {
    "modo_gps": "brt",
    "fonte_gps": "sonda",
    "15_minutos": False,
}

GPS_SONDA_SELECTOR = DBTSelector(
    name="gps",
    initial_datetime=datetime(2026, 5, 6, 10, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_sonda",
    incremental_delay_hours=1,
    redis_key_suffix="sonda",
    data_sources=[sonda_constants.SONDA_REGISTROS_SOURCE],
)
