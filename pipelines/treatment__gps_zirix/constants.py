# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps zirix
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__zirix_realocacao import constants as realocacao_constants
from pipelines.capture__zirix_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

GPS_DAILY_TEST = DBTTest(
    test_select="gps_onibus_zirix",
    delay_days_start=1,
    delay_days_end=1,
    truncate_date=True,
)

GPS_ZIRIX_SELECTOR = DBTSelector(
    name="gps_zirix",
    initial_datetime=datetime(2025, 5, 27, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_zirix",
    incremental_delay_hours=1,
    data_sources=[
        registros_constants.ZIRIX_REGISTROS_SOURCE,
        realocacao_constants.ZIRIX_REALOCACAO_SOURCE,
    ],
    post_test=GPS_DAILY_TEST,
)
