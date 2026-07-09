# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors do Rio Rotativo.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

# TODO: adicionar data_sources

RIOROTATIVO_DIARIO_SELECTOR = DBTSelector(
    name="riorotativo_diario",
    initial_datetime=datetime(2026, 7, 8, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo",
)

RIOROTATIVO_SNAPSHOT_SELECTOR = DBTSelector(
    name="riorotativo_snapshot",
    initial_datetime=datetime(2026, 7, 8, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo",
)
