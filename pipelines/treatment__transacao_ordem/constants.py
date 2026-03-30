# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector transacao_ordem
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_transacao_ordem import constants as transacao_ordem_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__financeiro_bilhetagem import constants as financeiro_bilhetagem_constants

TRANSACAO_ORDEM_SELECTOR = DBTSelector(
    name="transacao_ordem",
    initial_datetime=datetime(2024, 11, 21, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__transacao_ordem",
    data_sources=[
        transacao_ordem_constants.JAE_TRANSACAO_ORDEM_SOURCE,
        financeiro_bilhetagem_constants.FINANCEIRO_BILHETAGEM_SELECTOR,
    ],
)
