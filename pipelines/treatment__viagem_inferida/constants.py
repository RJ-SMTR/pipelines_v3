# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de viagem inferida
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__gps_conecta import constants as gps_conecta_constants
from pipelines.treatment__gps_zirix import constants as gps_zirix_constants
from pipelines.treatment__planejamento_diario import constants as planejamento_constants

ADDITIONAL_VARS = {"tipo_materializacao": "monitoramento"}

VIAGEM_INFERIDA_SELECTOR = DBTSelector(
    name="viagem_inferida",
    initial_datetime=datetime(2026, 8, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    incremental_delay_hours=24,
    flow_folder_name="treatment__viagem_inferida",
    data_sources=[
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR,
        gps_conecta_constants.GPS_CONECTA_SELECTOR,
        gps_zirix_constants.GPS_ZIRIX_SELECTOR,
    ],
)
