# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors dos dados auxiliares Rio Rotativo Digital.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_riorotativo_auxiliar import constants as riorotativo_aux_constants
from pipelines.capture__riorotativo_credenciados import (
    constants as riorotativo_credenciados_constants,
)
from pipelines.capture__riorotativo_vagas import constants as riorotativo_vagas_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

RIOROTATIVO_AUX_TEST = DBTTest(
    test_select=(
        "guardador_veiculo_riorotativo "
        "guardador_veiculo_riorotativo_historico "
        "agente_verificacao_riorotativo "
        "agente_verificacao_riorotativo_historico "
        "entidade_credenciadora_riorotativo_historico "
        "area_estacionamento_riorotativo "
        "perfil_funcionamento_riorotativo "
        "perfil_funcionamento_riorotativo_historico"
    ),
    truncate_date=True,
)

RIOROTATIVO_AUX_SELECTOR = DBTSelector(
    name="riorotativo_auxiliar",
    initial_datetime=datetime(2026, 7, 14, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo_auxiliar",
    data_sources=[
        *riorotativo_credenciados_constants.RIOROTATIVO_CREDENCIADOS_SOURCES,
        *riorotativo_vagas_constants.RIOROTATIVO_VAGAS_SOURCES,
        *riorotativo_aux_constants.JAE_RIOROTATIVO_AUXILIAR_SOURCES,
    ],
    post_test=RIOROTATIVO_AUX_TEST,
)

SNAPSHOT_RIOROTATIVO_AUX_SELECTOR = DBTSelector(
    name="snapshot_riorotativo_auxiliar",
    initial_datetime=datetime(2026, 7, 14, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo_auxiliar",
)
