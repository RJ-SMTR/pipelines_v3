# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector extrato_cliente_cartao
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_lancamento import constants as lancamento_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__cadastro import constants as cadastro_constants

EXTRATO_CLIENTE_CARTAO_SELECTOR = DBTSelector(
    name="extrato_cliente_cartao",
    initial_datetime=datetime(2025, 10, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__extrato_cliente_cartao",
    incremental_delay_hours=1,
    data_sources=[cadastro_constants.CADASTRO_SELECTOR, lancamento_constants.LANCAMENTO_SOURCE],
)
