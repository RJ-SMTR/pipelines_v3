# -*- coding: utf-8 -*-
"""Constantes do flow treatment__remuneracao_openfisca."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

# Início alinhado aos parâmetros I.9 / Tab. 2 no OpenFisca (simulação A0 jul/2026).
REMUNERACAO_INITIAL_DATETIME = datetime(
    2026, 7, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)
)

# Ao reativar data_sources, definir WAIT_* aqui e COPY das pastas dos flows
# no Dockerfile (DBTSelector lê prefect.yaml no import via flow_folder_name).
REMUNERACAO_OPENFISCA_SELECTOR = DBTSelector(
    name="remuneracao_openfisca",
    initial_datetime=REMUNERACAO_INITIAL_DATETIME,
    incremental_delay_hours=5 * 24,
    flow_folder_name="treatment__remuneracao_openfisca",
)
