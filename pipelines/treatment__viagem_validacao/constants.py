# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de viagem validação
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

# from pipelines.treatment__gps_cittati import constants as gps_cittati_constants
from pipelines.treatment__gps_conecta import constants as gps_conecta_constants
from pipelines.treatment__gps_zirix import constants as gps_zirix_constants
from pipelines.treatment__planejamento_diario import constants as planejamento_constants

VIAGEM_VALIDACAO_DELAY_HOURS = 48

WAIT_VIAGEM_INFORMADA_SELECTOR = DBTSelector(
    name="viagem_informada",
    initial_datetime=datetime(2024, 10, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__viagem_informada",
    incremental_delay_hours=-VIAGEM_VALIDACAO_DELAY_HOURS,
)

VIAGEM_VALIDACAO_SELECTOR = DBTSelector(
    name="viagem_validacao",
    initial_datetime=datetime(2024, 10, 12, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__viagem_validacao",
    incremental_delay_hours=VIAGEM_VALIDACAO_DELAY_HOURS,
    data_sources=[
        WAIT_VIAGEM_INFORMADA_SELECTOR,
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR,
        gps_conecta_constants.GPS_CONECTA_SELECTOR,
        # gps_cittati_constants.GPS_CITTATI_SELECTOR,
        gps_zirix_constants.GPS_ZIRIX_SELECTOR,
    ],
)
