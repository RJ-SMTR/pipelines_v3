# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector validacao_dados_jae
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__integracao import constants as integracao_constants
from pipelines.treatment__transacao import constants as transacao_constants

VALIDACAO_DADOS_JAE_DAILY_TEST = DBTTest(
    test_select="integracao_nao_realizada",
    test_descriptions={
        "integracao_nao_realizada": {
            "unique": {"description": "Todos os registros são únicos"},
        },
    },
    truncate_date=True,
    delay_days_start=1,
)

VALIDACAO_DADOS_JAE_SELECTOR = DBTSelector(
    name="validacao_dados_jae",
    initial_datetime=datetime(2024, 12, 30, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__validacao_dados_jae",
    post_test=VALIDACAO_DADOS_JAE_DAILY_TEST,
    data_sources=[
        transacao_constants.TRANSACAO_SELECTOR,
        integracao_constants.INTEGRACAO_SELECTOR,
    ],
)
