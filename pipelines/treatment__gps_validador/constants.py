# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector gps_validador
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_gps_validador import constants as gps_validador_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__cadastro import constants as cadastro_constants

GPS_VALIDADOR_POST_TEST = DBTTest(
    test_select="gps_validador gps_validador_van",
    test_descriptions={
        "gps_validador": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
        "gps_validador_van": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
    },
    truncate_date=True,
)

GPS_VALIDADOR_SELECTOR = DBTSelector(
    name="gps_validador",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gps_validador",
    incremental_delay_hours=1,
    data_sources=[
        gps_validador_constants.GPS_VALIDADOR_SOURCE,
        cadastro_constants.CADASTRO_SELECTOR,
    ],
    post_test=GPS_VALIDADOR_POST_TEST,
)
