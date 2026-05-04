# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector datario.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

DATARIO_SELECTOR = DBTSelector(
    name="datario",
    initial_datetime=datetime(2024, 12, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__datario",
)
