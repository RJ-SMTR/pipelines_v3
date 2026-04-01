# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector pagamento_cct
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__cct_pagamento import constants as cct_pagamento_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

PAGAMENTO_CCT_SELECTOR = DBTSelector(
    name="pagamento_cct",
    initial_datetime=datetime(2025, 10, 8, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__pagamento_cct",
    data_sources=[cct_pagamento_constants.PAGAMENTO_SOURCES],
)
