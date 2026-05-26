# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector veiculo_dia
"""

from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__veiculo_sppo_registro_agente_verao import (
    constants as veiculo_sppo_registro_agente_verao_constants,
)
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__cadastro_veiculo import constants as cadastro_veiculo_constants
from pipelines.treatment__monitoramento_veiculo import constants as monitoramento_veiculo_constants

VEICULO_DIA_INCREMENTAL_DELAY_HOURS = 24 * 7

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

wait_monitoramento_veiculo = deepcopy(
    monitoramento_veiculo_constants.MONITORAMENTO_VEICULO_SELECTOR
)
wait_monitoramento_veiculo.incremental_delay_hours = -VEICULO_DIA_INCREMENTAL_DELAY_HOURS
wait_cadastro_veiculo = deepcopy(cadastro_veiculo_constants.CADASTRO_VEICULO_SELECTOR)
wait_cadastro_veiculo.incremental_delay_hours = -VEICULO_DIA_INCREMENTAL_DELAY_HOURS


VEICULO_DIA_TEST = DBTTest(
    test_select="veiculo_dia",
    test_descriptions=VEICULO_DIA_CHECKS_LIST,
    truncate_date=True,
)

VEICULO_DIA_SELECTOR = DBTSelector(
    name="veiculo_dia",
    initial_datetime=datetime(2025, 6, 23, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    incremental_delay_hours=VEICULO_DIA_INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__veiculo_dia",
    post_test=VEICULO_DIA_TEST,
    data_sources=[
        wait_monitoramento_veiculo,
        wait_cadastro_veiculo,
        *veiculo_sppo_registro_agente_verao_constants.SPPO_REGISTRO_AGENTE_VERAO_SOURCES,
    ],
)


SNAPSHOT_VEICULO_DIA_SELECTOR = DBTSelector(
    name="snapshot_veiculo_dia",
    initial_datetime=datetime(2025, 6, 23, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__veiculo_dia",
)
