# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector monitoramento_temperatura
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

MONITORAMENTO_TEMPERATURA_CHECKS_LIST = {
    "temperatura_inmet": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "aux_viagem_temperatura": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.unique_combination_of_columns__aux_viagem_temperatura": {
            "description": "Todos os registros são únicos"
        },
    },
    "aux_veiculo_falha_ar_condicionado": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.unique_combination_of_columns__aux_veiculo_falha_ar_condicionado": {
            "description": "Todos os registros são únicos"
        },
    },
    "veiculo_regularidade_temperatura_dia": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.unique_combination_of_columns__veiculo_regularidade_temperatura_dia": {
            "description": "Todos os registros são únicos"
        },
    },
    "temperatura_alertario": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
    "temperatura": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "test_completude__temperatura": {
            "description": "Há pelo menos uma temperatura não nula registrada em alguma das estações do Rio de Janeiro em cada uma das 24 horas do dia"
        },
    },
}

MONITORAMENTO_TEMPERATURA_TEST = DBTTest(
    test_select="temperatura_inmet temperatura_alertario aux_viagem_temperatura aux_veiculo_falha_ar_condicionado veiculo_regularidade_temperatura_dia temperatura",
    exclude="test_check_regularidade_temperatura__viagem_regularidade_temperatura test_consistencia_indicadores_temperatura__viagem_regularidade_temperatura",
    test_descriptions=MONITORAMENTO_TEMPERATURA_CHECKS_LIST,
    truncate_date=True,
)

MONITORAMENTO_TEMPERATURA_SELECTOR = DBTSelector(
    name="monitoramento_temperatura",
    initial_datetime=datetime(2025, 7, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__monitoramento_temperatura",
    post_test=MONITORAMENTO_TEMPERATURA_TEST,
    # Depende do pipeline de monitoramento_veiculo
    # As dependências dbt são gerenciadas automaticamente via dbt manifest
)

SNAPSHOT_TEMPERATURA_SELECTOR = DBTSelector(
    name="snapshot_temperatura",
    initial_datetime=datetime(2025, 7, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__monitoramento_temperatura",
)
