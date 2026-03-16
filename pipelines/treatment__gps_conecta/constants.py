# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps conecta
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__conecta_realocacao import constants as realocacao_constants
from pipelines.capture__conecta_registros import constants as registros_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

GPS_POST_CHECKS_LIST = {
    "gps": {
        "check_gps_treatment__gps": {
            "description": "Todos os dados de GPS foram devidamente tratados"
        },
        "dbt_utils.unique_combination_of_columns__gps": {
            "description": "Todos os registros são únicos"
        },
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
    },
}

GPS_DAILY_TEST = DBTTest(
    test_select="gps",
    test_descriptions=GPS_POST_CHECKS_LIST,
    delay_days_start=1,
    delay_days_end=1,
    truncate_date=True,
)

GPS_CONECTA_SELECTOR = DBTSelector(
    name="gps",
    initial_datetime=datetime(2025, 5, 27, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_conecta",
    incremental_delay_hours=1,
    redis_key_suffix="conecta",
    data_sources=[
        registros_constants.CONECTA_REGISTROS_SOURCE,
        realocacao_constants.CONECTA_REALOCACAO_SOURCE,
    ],
    post_test=GPS_DAILY_TEST,
)
