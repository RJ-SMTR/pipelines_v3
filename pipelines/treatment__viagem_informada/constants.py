# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de viagem informada
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__conecta_sistema_rio_viagem_informada import (
    constants as conecta_sistema_rio_constants,
)
from pipelines.capture__maxtrack_viagem_informada import constants as maxtrack_constants
from pipelines.capture__rioonibus_viagem_informada import constants as rioonibus_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

VIAGEM_INFORMADA_TEST = DBTTest(
    test_select="viagem_informada_monitoramento",
    truncate_date=True,
)

VIAGEM_INFORMADA_SELECTOR = DBTSelector(
    name="viagem_informada",
    initial_datetime=datetime(2024, 10, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__viagem_informada",
    data_sources=[
        rioonibus_constants.VIAGEM_INFORMADA_SOURCE,
        maxtrack_constants.VIAGEM_INFORMADA_SOURCE,
        conecta_sistema_rio_constants.VIAGEM_INFORMADA_SOURCE,
    ],
    post_test=VIAGEM_INFORMADA_TEST,
)
