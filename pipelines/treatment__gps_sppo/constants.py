# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__sppo_realocacao import constants as realocacao_constants
from pipelines.capture__sppo_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

GPS_DAILY_TEST = DBTTest(
    test_select="gps_onibus_sppo",
    delay_days_start=1,
    delay_days_end=1,
    truncate_date=True,
)

GPS_SPPO_SELECTOR = DBTSelector(
    name="gps_sppo",
    initial_datetime=datetime(2026, 4, 29, 10, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_sppo",
    incremental_delay_hours=1,
    data_sources=[
        registros_constants.SPPO_REGISTROS_SOURCE,
        realocacao_constants.SPPO_REALOCACAO_SOURCE,
    ],
    post_test=GPS_DAILY_TEST,
)
