# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors do Rio Rotativo.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__riorotativo_credenciados import (
    constants as riorotativo_credenciados_constants,
)
from pipelines.capture__riorotativo_vagas import constants as riorotativo_vagas_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

RIOROTATIVO_DIARIO_SELECTOR = DBTSelector(
    name="riorotativo_diario",
    initial_datetime=datetime(2026, 7, 14, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo",
    data_sources=[
        *riorotativo_credenciados_constants.RIOROTATIVO_CREDENCIADOS_SOURCES,
        *riorotativo_vagas_constants.RIOROTATIVO_VAGAS_SOURCES,
    ],
)
