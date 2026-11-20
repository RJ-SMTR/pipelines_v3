# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector mit_ipea_project
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__transacao import constants as transacao_constants

MIT_IPEA_PROJECT_SELECTOR = DBTSelector(
    name="mit_ipea_project",
    initial_datetime=datetime(2026, 11, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__mit_ipea_project",
    data_sources=[transacao_constants.TRANSACAO_SELECTOR],
)
