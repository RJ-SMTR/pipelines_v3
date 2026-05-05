# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector operadora_cnpj
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

OPERADORA_CNPJ_SELECTOR = DBTSelector(
    name="operadora_cnpj",
    initial_datetime=datetime(2026, 5, 4, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__operadora_cnpj",
)
