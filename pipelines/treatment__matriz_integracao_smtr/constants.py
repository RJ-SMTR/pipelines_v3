# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector integracao
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

MATRIZ_INTEGRACAO_SMTR_SELECTOR = DBTSelector(
    name="matriz_integracao_smtr",
    initial_datetime=datetime(2024, 12, 30, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__matriz_integracao_smtr",
)
