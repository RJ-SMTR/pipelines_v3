# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de viagem informada
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__rioonibus_viagem_informada import constants as rioonibus_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__planejamento_diario import constants as planejamento_constants

VIAGEM_INFORMADA_SELECTOR = DBTSelector(
    name="viagem_informada",
    initial_datetime=datetime(2024, 10, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__viagem_informada",
    data_sources=[
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR,
        rioonibus_constants.VIAGEM_INFORMADA_SOURCE,
    ],
)
