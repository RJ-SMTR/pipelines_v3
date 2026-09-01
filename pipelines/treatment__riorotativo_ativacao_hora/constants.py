# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors dos dados agregados do Rio Rotativo Digital.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

RIOROTATIVO_ATIVACAO_HORA_SELECTOR = DBTSelector(
    name="riorotativo_ativacao_hora",
    initial_datetime=datetime(2026, 8, 27, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo_ativacao_hora",
)
