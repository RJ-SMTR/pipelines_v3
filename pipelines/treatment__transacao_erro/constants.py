# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector transacao_erro
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_transacao_erro import constants as transacao_erro_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__cadastro import constants as cadastro_constants

TRANSACAO_ERRO_SELECTOR = DBTSelector(
    name="transacao_erro",
    initial_datetime=datetime(2025, 3, 4, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__transacao_erro",
    incremental_delay_hours=1,
    data_sources=[
        transacao_erro_constants.JAE_TRANSACAO_ERRO_SOURCE,
        cadastro_constants.CADASTRO_SELECTOR,
    ],
)
