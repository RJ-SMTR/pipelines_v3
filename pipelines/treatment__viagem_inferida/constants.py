# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de viagem inferida
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__gps_cittati import constants as gps_cittati_constants
from pipelines.treatment__gps_conecta import constants as gps_conecta_constants
from pipelines.treatment__gps_maxtrack import constants as gps_maxtrack_constants
from pipelines.treatment__gps_sonda import constants as gps_sonda_constants
from pipelines.treatment__gps_validador import constants as gps_validador_constants
from pipelines.treatment__gps_zirix import constants as gps_zirix_constants
from pipelines.treatment__planejamento_diario import constants as planejamento_constants

ADDITIONAL_VARS = {"tipo_materializacao": "monitoramento"}

VIAGEM_INFERIDA_SELECTOR = DBTSelector(
    name="viagem_inferida",
    initial_datetime=datetime(2026, 8, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__viagem_inferida",
    incremental_delay_hours=1,
    data_sources=[
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR,
        gps_conecta_constants.GPS_CONECTA_SELECTOR,
        gps_cittati_constants.GPS_CITTATI_SELECTOR,
        gps_maxtrack_constants.GPS_MAXTRACK_SELECTOR,
        gps_zirix_constants.GPS_ZIRIX_SELECTOR,
        gps_sonda_constants.GPS_SONDA_SELECTOR,
        gps_validador_constants.GPS_VALIDADOR_SELECTOR,
    ],
)
