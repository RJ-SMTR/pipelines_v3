# -*- coding: utf-8 -*-
"""Valores constantes para materialização do selector GPS Conecta Sistema RIO."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__conecta_sistema_rio_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

GPS_DAILY_TEST = DBTTest(
    test_select="gps_v2_onibus_conecta_sistema_rio",
    delay_days_start=1,
    delay_days_end=1,
    truncate_date=True,
)

GPS_CONECTA_SISTEMA_RIO_SELECTOR = DBTSelector(
    name="gps_v2_conecta_sistema_rio",
    initial_datetime=datetime(2026, 10, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_conecta_sistema_rio",
    incremental_delay_hours=1,
    data_sources=[registros_constants.REGISTROS_SOURCE],
    post_test=GPS_DAILY_TEST,
)
