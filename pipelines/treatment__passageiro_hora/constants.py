# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_validador
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

PASSAGEIRO_HORA_DAILY_TEST = DBTTest(
    test_select="passageiro_hora passageiro_tile_hora transacao_gratuidade_estudante_municipal",
    test_descriptions={
        "passageiro_hora": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "passageiro_tile_hora": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "transacao_gratuidade_estudante_municipal": {
            "sincronizacao_tabelas__transacao_gratuidade_estudante_municipal": {
                "description": "Tabela `transacao_gratuidade_estudante_municipal` sincronizada"
            }
        },
    },
    truncate_date=True,
)

PASSAGEIRO_HORA_SELECTOR = DBTSelector(
    name="passageiro_hora",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__passageiro_hora",
    post_test=PASSAGEIRO_HORA_DAILY_TEST,
)
