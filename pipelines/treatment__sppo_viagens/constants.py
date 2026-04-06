# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do seletor viagens_sppo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

VIAGENS_SPPO_SELECTOR = DBTSelector(
    name="viagens_sppo",
    initial_datetime=datetime(2026, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__sppo_viagens",
)

VIAGENS_SPPO_SNAPSHOT_SELECTOR = DBTSelector(
    name="snapshot_viagem",
    initial_datetime=datetime(2026, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__sppo_viagens",
)
