# -*- coding: utf-8 -*-
"""Constantes do flow treatment__remuneracao_openfisca."""

from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.treatment__planejamento_diario import constants as planejamento_constants

# Início alinhado aos parâmetros I.9 / Tab. 2 no OpenFisca (simulação A0 jul/2026).
REMUNERACAO_INITIAL_DATETIME = datetime(
    2026, 7, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)
)

# WIP/teste: base = viagem_completa (via viagens_sppo). Trocar de volta para
# viagem_validacao / viagem_valida quando houver dados.
WAIT_VIAGENS_SPPO_SELECTOR = DBTSelector(
    name="viagens_sppo",
    initial_datetime=datetime(2024, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__sppo_viagens",
    incremental_delay_hours=5 * 24,
)

REMUNERACAO_OPENFISCA_SELECTOR = DBTSelector(
    name="remuneracao_openfisca",
    initial_datetime=REMUNERACAO_INITIAL_DATETIME,
    incremental_delay_hours=5 * 24,
    flow_folder_name="treatment__remuneracao_openfisca",
    # data_sources=[
    #     WAIT_VIAGENS_SPPO_SELECTOR,
    #     deepcopy(planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR),
    # ],
)
