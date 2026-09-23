# -*- coding: utf-8 -*-
"""Constantes do flow treatment__remuneracao_openfisca."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

ADDITIONAL_VARS = {"sistema": "rio"}

REMUNERACAO_OPENFISCA_SELECTOR = DBTSelector(
    name="remuneracao_openfisca",
    initial_datetime=datetime(2026, 7, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__remuneracao_openfisca",
)
