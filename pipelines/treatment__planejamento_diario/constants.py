# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector planejamento_diario.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

PLANEJAMENTO_DIARIO_TEST_EXCLUDE = (
    "tag:freshness "
    "check_km_planejada__sumario_faixa_servico_dia "
    "check_km_planejada__sumario_faixa_servico_dia_pagamento "
    "check_km_planejada__viagem_planejada "
    "tecnologia_servico viagem_informada_monitoramento viagens_remuneradas"
)

PLANEJAMENTO_DIARIO_TEST = DBTTest(
    test_select="servico_planejado_faixa_horaria viagem_planejada_planejamento_dia",
    exclude=PLANEJAMENTO_DIARIO_TEST_EXCLUDE,
    truncate_date=True,
)

PLANEJAMENTO_DIARIO_SELECTOR = DBTSelector(
    name="planejamento_diario",
    initial_datetime=datetime(2024, 9, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__planejamento_diario",
    post_test=PLANEJAMENTO_DIARIO_TEST,
)

SNAPSHOT_PLANEJAMENTO_SELECTOR = DBTSelector(
    name="snapshot_planejamento",
    initial_datetime=datetime(2024, 9, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__planejamento_diario",
)
