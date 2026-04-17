# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_15_minutos SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__sppo_realocacao import constants as realocacao_constants
from pipelines.capture__sppo_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

GPS_15_MINUTOS_SPPO_SELECTOR = DBTSelector(
    name="gps_15_minutos",
    initial_datetime=datetime(2026, 4, 16, 17, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_15_minutos_sppo",
    redis_key_suffix="sppo",
    data_sources=[
        registros_constants.SPPO_REGISTROS_SOURCE,
        realocacao_constants.SPPO_REALOCACAO_SOURCE,
    ],
)
