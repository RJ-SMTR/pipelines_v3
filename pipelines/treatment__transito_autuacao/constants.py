# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors de autuação de trânsito
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__serpro_autuacao import constants as serpro_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

TRANSITO_AUTUACAO_SELECTOR = DBTSelector(
    name="transito_autuacao",
    initial_datetime=datetime(2025, 3, 29, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__transito_autuacao",
    data_sources=[serpro_constants.SERPRO_AUTUACAO_SOURCE],
)

SNAPSHOT_TRANSITO_SELECTOR = DBTSelector(
    name="snapshot_transito",
    initial_datetime=datetime(2023, 5, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__transito_autuacao",
    data_sources=[TRANSITO_AUTUACAO_SELECTOR],
)
