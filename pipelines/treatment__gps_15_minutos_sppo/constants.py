# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_15_minutos SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

ADDITIONAL_VARS = {
    "modo_gps": "onibus",
    "fonte_gps": "sppo",
    "15_minutos": True,
}

GPS_15_MINUTOS_SPPO_SELECTOR = DBTSelector(
    name="gps_15_minutos",
    initial_datetime=datetime(2026, 4, 29, 10, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_15_minutos_sppo",
    redis_key_suffix="sppo",
)
