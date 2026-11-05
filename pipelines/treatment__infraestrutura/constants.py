# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector infraestrutura.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

INFRAESTRUTURA_SELECTOR = DBTSelector(
    name="infraestrutura",
    initial_datetime=datetime(2025, 6, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__infraestrutura",
)
