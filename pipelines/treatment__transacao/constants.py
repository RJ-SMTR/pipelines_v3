# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector transacao
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_transacao import constants as transacao_constants
from pipelines.capture__jae_transacao_riocard import constants as transacao_riocard_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__cadastro import constants as cadastro_constants
from pipelines.treatment__integracao import constants as integracao_constants

TRANSACAO_POST_TEST = DBTTest(
    test_select="aux_gratuidade_info transacao transacao_riocard",
    exclude="sincronizacao_tabelas__transacao_gratuidade_estudante_municipal",
    test_descriptions={
        "transacao": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
        "transacao_riocard": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
    },
    truncate_date=True,
    delay_days_start=1,
)

TRANSACAO_SELECTOR = DBTSelector(
    name="transacao",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__transacao",
    incremental_delay_hours=1,
    data_sources=[
        transacao_constants.TRANSACAO_SOURCE,
        cadastro_constants.CADASTRO_SELECTOR,
        transacao_riocard_constants.TRANSACAO_RIOCARD_SOURCE,
        integracao_constants.INTEGRACAO_SELECTOR,
    ],
    post_test=TRANSACAO_POST_TEST,
)
