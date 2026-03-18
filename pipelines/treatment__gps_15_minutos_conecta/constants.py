# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_15_minutos conecta
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__conecta_realocacao import constants as realocacao_constants
from pipelines.capture__conecta_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

GPS_15_MINUTOS_CONECTA_SELECTOR = DBTSelector(
    name="gps_15_minutos",
    initial_datetime=datetime(2025, 5, 27, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_15_minutos_conecta",
    redis_key_suffix="conecta",
    data_sources=[
        registros_constants.CONECTA_REGISTROS_SOURCE,
        realocacao_constants.CONECTA_REALOCACAO_SOURCE,
    ],
)
