# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors dos dados de operação do Rio Rotativo Digital.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_fiscalizacao_veiculo import constants as fiscalizacao_constants
from pipelines.capture__jae_movimento_estacionamento_veiculo import (
    constants as movimento_estacionamento_veiculo_constants,
)
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__riorotativo_auxiliar import constants as riorotativo_auxiliar_constants

RIOROTATIVO_OPERACAO_TEST = DBTTest(
    test_select=("ativacao_riorotativo verificacao_guardador_veiculo_riorotativo"),
    truncate_date=True,
    delay_days_start=1,
)

RIOROTATIVO_OPERACAO_SELECTOR = DBTSelector(
    name="riorotativo_operacao",
    initial_datetime=datetime(2026, 7, 29, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo_operacao",
    data_sources=[
        movimento_estacionamento_veiculo_constants.MOVIMENTO_ESTACIONAMENTO_VEICULO_SOURCE,
        fiscalizacao_constants.FISCALIZACAO_VEICULO_SOURCE,
        riorotativo_auxiliar_constants.RIOROTATIVO_AUX_SELECTOR,
    ],
    post_test=RIOROTATIVO_OPERACAO_TEST,
    incremental_delay_hours=1,
)
