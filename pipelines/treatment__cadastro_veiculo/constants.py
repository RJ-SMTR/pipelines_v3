# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do cadastro de veículos
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__monitoramento_veiculo import (
    constants as monitoramento_veiculo_constants,
)

CADASTRO_VEICULO_CHECKS_LIST = {
    "staging_licenciamento_stu": {
        "dbt_utils__recency__data__staging_licenciamento_stu": {
            "description": "Arquivo de licenciamento STU ingerido nas últimas 24 horas"
        },
    },
}

CADASTRO_VEICULO_TEST = DBTTest(
    test_select="staging_licenciamento_stu",
    test_descriptions=CADASTRO_VEICULO_CHECKS_LIST,
    truncate_date=True,
)

CADASTRO_VEICULO_SELECTOR = DBTSelector(
    name="cadastro_veiculo",
    initial_datetime=datetime(2025, 6, 23, 6, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cadastro_veiculo",
    data_sources=[monitoramento_veiculo_constants.MONITORAMENTO_VEICULO_SELECTOR],
    post_test=CADASTRO_VEICULO_TEST,
)

SNAPSHOT_CADASTRO_VEICULO_SELECTOR = DBTSelector(
    name="snapshot_cadastro_veiculo",
    initial_datetime=datetime(2025, 6, 23, 6, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cadastro_veiculo",
)
