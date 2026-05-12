# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector veiculo_dia
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

VEICULO_DIA_SELECTOR = DBTSelector(
    name="veiculo_dia",
    initial_datetime=datetime(2025, 6, 23, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    incremental_delay_hours=24 * 7,
    flow_folder_name="treatment__veiculo_dia",
)

SNAPSHOT_VEICULO_DIA_SELECTOR = DBTSelector(
    name="snapshot_veiculo_dia",
    initial_datetime=datetime(2025, 6, 23, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__veiculo_dia",
)

VEICULO_DIA_CHECKS_LIST = {
    "veiculo_dia": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "dbt_utils.unique_combination_of_columns__data_id_veiculo__veiculo_dia": {
            "description": "Todos os registros são únicos"
        },
        "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__veiculo_dia": {
            "description": "Todas as datas possuem dados"
        },
        "test_check_veiculo_lacre__veiculo_dia": {
            "description": "Todos os veículos lacrados têm dados consistentes entre `veiculo_dia` e veiculo_fiscalizacao_lacre"
        },
    }
}

VEICULO_DIA_TEST = DBTTest(
    test_select="veiculo_dia",
    test_descriptions=VEICULO_DIA_CHECKS_LIST,
    truncate_date=True,
)
