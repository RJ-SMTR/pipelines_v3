# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector controle_financeiro
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from capture__smtr_controle_financeiro import constants as capture_constants

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

CONTROLE_FINANCEIRO_SELECTOR = DBTSelector(
    name="controle_financeiro",
    initial_datetime=datetime(2026, 10, 14, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__controle_financeiro",
    data_sources=capture_constants.CONTROLE_FINANCEIRO_SOURCES,
)
