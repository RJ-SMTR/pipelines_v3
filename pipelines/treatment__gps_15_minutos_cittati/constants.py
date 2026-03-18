# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_15_minutos cittati
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__cittati_realocacao import constants as realocacao_constants
from pipelines.capture__cittati_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

GPS_15_MINUTOS_CITTATI_SELECTOR = DBTSelector(
    name="gps_15_minutos",
    initial_datetime=datetime(2025, 5, 27, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_15_minutos_cittati",
    redis_key_suffix="cittati",
    data_sources=[
        registros_constants.CITTATI_REGISTROS_SOURCE,
        realocacao_constants.CITTATI_REALOCACAO_SOURCE,
    ],
)
