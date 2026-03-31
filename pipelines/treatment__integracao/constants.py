# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector integracao
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

INTEGRACAO_DAILY_TEST = DBTTest(
    test_select="integracao",
    test_descriptions={
        "integracao": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__integracao": {
                "description": "Todos os registros são únicos"
            },
        },
    },
    truncate_date=True,
    delay_days_start=1,
)


INTEGRACAO_SELECTOR = DBTSelector(
    name="integracao",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__integracao",
    post_test=INTEGRACAO_DAILY_TEST,
)
