# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector cliente_cpf
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

CLIENTE_CPF_SELECTOR = DBTSelector(
    name="cliente_cpf",
    initial_datetime=datetime(2026, 1, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cliente_cpf",
)
