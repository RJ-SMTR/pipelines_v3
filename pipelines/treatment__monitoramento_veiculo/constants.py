# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de monitoramento de veículo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__veiculo_fiscalizacao import constants as veiculo_fiscalizacao_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

MONITORAMENTO_VEICULO_CHECKS_LIST = {
    "veiculo_fiscalizacao_lacre": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.relationships_where__id_auto_infracao__veiculo_fiscalizacao_lacre": {
            "description": "Todos os ids de auto infração estão na tabela de autuação"
        },
        "dbt_utils.unique_combination_of_columns__id_veiculo__veiculo_fiscalizacao_lacre": {
            "description": "Todos os registros são únicos para combinação `id_veiculo`, `data_inicio_lacre` e `id_auto_infracao`"
        },
        "dbt_utils.unique_combination_of_columns__placa__veiculo_fiscalizacao_lacre": {
            "description": "Todos os registros são únicos para combinação `placa`, `data_inicio_lacre` e `id_auto_infracao`"
        },
    },
    "autuacao_disciplinar_historico": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.relationships_where__id_auto_infracao__autuacao_disciplinar_historico": {
            "description": "Todos as autuações geraram lacre corretamente"
        },
    },
}

MONITORAMENTO_VEICULO_TEST = DBTTest(
    test_select="veiculo_fiscalizacao_lacre autuacao_disciplinar_historico",
    exclude="test_check_veiculo_lacre__veiculo_dia",
    test_descriptions=MONITORAMENTO_VEICULO_CHECKS_LIST,
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
