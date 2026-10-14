# -*- coding: utf-8 -*-
"""Valores constantes para materialização do selector GPS Maxtrack."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__maxtrack_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

ADDITIONAL_VARS = {
    "modo_gps": "onibus",
    "fonte_gps": "maxtrack",
    "15_minutos": False,
}


GPS_DAILY_TEST = DBTTest(
    test_select="gps.v2",
    delay_days_start=1,
    delay_days_end=1,
    truncate_date=True,
    additional_vars=ADDITIONAL_VARS,
)

GPS_MAXTRACK_SELECTOR = DBTSelector(
    name="gps_v2",
    initial_datetime=datetime(2026, 8, 22, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_maxtrack",
    incremental_delay_hours=1,
    redis_key_suffix="maxtrack",
    data_sources=[registros_constants.MAXTRACK_REGISTROS_SOURCE],
    post_test=GPS_DAILY_TEST,
)
