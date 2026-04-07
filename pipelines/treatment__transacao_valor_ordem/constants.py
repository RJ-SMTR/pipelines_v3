# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector transacao_valor_ordem
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__integracao import constants as integracao_constants
from pipelines.treatment__transacao import constants as transacao_constants
from pipelines.treatment__transacao_ordem import constants as transacao_ordem_constants

TRANSACAO_VALOR_ORDEM_DAILY_TEST = DBTTest(
    test_select="transacao_valor_ordem",
    test_descriptions={
        "transacao_valor_ordem": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__transacao_valor_ordem": {
                "description": "Todos os registros são únicos"
            },
            "transacao_valor_ordem_completa__transacao_valor_ordem": {
                "description": "Todas ordens estão presentes na tabela"
            },
        }
    },
    truncate_date=True,
)

TRANSACAO_VALOR_ORDEM_SELECTOR = DBTSelector(
    name="transacao_valor_ordem",
    initial_datetime=datetime(2025, 2, 4, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__transacao_valor_ordem",
    post_test=TRANSACAO_VALOR_ORDEM_DAILY_TEST,
    data_sources=[
        transacao_ordem_constants.TRANSACAO_ORDEM_SELECTOR,
        transacao_constants.TRANSACAO_SELECTOR,
        integracao_constants.INTEGRACAO_SELECTOR,
    ],
)
