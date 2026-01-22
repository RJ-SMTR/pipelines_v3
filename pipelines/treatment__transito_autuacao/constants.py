# -*- coding: utf-8 -*-
"""
Valores constantes para pipeline treatment__transito_autuacao
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as common_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

TRANSITO_AUTUACAO_SELECTOR = DBTSelector(
    name="transito_autuacao",
    initial_datetime=datetime(2025, 3, 29, tzinfo=ZoneInfo(common_constants.TIMEZONE)),
    flow_folder_name="treatment__transito_autuacao",
)
