# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector monitoramento_temperatura
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__monitoramento_veiculo import constants as monitoramento_veiculo_constants

MONITORAMENTO_TEMPERATURA_TEST = DBTTest(
    test_select="temperatura_inmet temperatura_alertario aux_viagem_temperatura aux_veiculo_falha_ar_condicionado veiculo_regularidade_temperatura_dia temperatura",
    exclude="test_check_regularidade_temperatura__viagem_regularidade_temperatura test_consistencia_indicadores_temperatura__viagem_regularidade_temperatura",
    truncate_date=True,
)

MONITORAMENTO_TEMPERATURA_SELECTOR = DBTSelector(
    name="monitoramento_temperatura",
    initial_datetime=datetime(2025, 7, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__monitoramento_temperatura",
    post_test=MONITORAMENTO_TEMPERATURA_TEST,
    data_sources=[monitoramento_veiculo_constants.MONITORAMENTO_VEICULO_SELECTOR],
)

SNAPSHOT_TEMPERATURA_SELECTOR = DBTSelector(
    name="snapshot_temperatura",
    initial_datetime=datetime(2025, 7, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__monitoramento_temperatura",
)
