# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de monitoramento de veículo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__veiculo_fiscalizacao_lacre import (
    constants as veiculo_fiscalizacao_constants,
)
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

MONITORAMENTO_VEICULO_TEST = DBTTest(
    test_select="veiculo_fiscalizacao_lacre autuacao_disciplinar_historico staging_infracao",
    exclude="test_check_veiculo_lacre__veiculo_dia",
    truncate_date=True,
)

MONITORAMENTO_VEICULO_SELECTOR = DBTSelector(
    name="monitoramento_veiculo",
    initial_datetime=datetime(2025, 5, 28, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__monitoramento_veiculo",
    data_sources=[veiculo_fiscalizacao_constants.VEICULO_LACRE_SOURCE],
    post_test=MONITORAMENTO_VEICULO_TEST,
)

SNAPSHOT_VEICULO_SELECTOR = DBTSelector(
    name="snapshot_veiculo",
    initial_datetime=datetime(2025, 5, 28, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__monitoramento_veiculo",
)
