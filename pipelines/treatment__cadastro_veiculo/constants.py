# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do cadastro de veículos
"""

from datetime import datetime

from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

CADASTRO_VEICULO_SELECTOR = DBTSelector(
    name="cadastro_veiculo",
    initial_datetime=datetime(2025, 6, 23, 6, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cadastro_veiculo",
)

SNAPSHOT_CADASTRO_VEICULO_SELECTOR = DBTSelector(
    name="snapshot_cadastro_veiculo",
    initial_datetime=datetime(2025, 6, 23, 6, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cadastro_veiculo",
)
