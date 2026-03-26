# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector financeiro_bilhetagem
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_ordem_pagamento import constants as ordem_pagamento_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__cadastro import constants as cadastro_constants

FINANCEIRO_BILHETAGEM_SELECTOR = DBTSelector(
    name="financeiro_bilhetagem",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__financeiro_bilhetagem",
    data_sources=[
        cadastro_constants.CADASTRO_SELECTOR,
        *ordem_pagamento_constants.JAE_ORDEM_PAGAMENTO_SOURCES,
    ],
)
