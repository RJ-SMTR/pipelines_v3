# -*- coding: utf-8 -*-
"""Constantes e configuração específicas do tratamento GTFS."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

GTFS_DISCORD_WEBHOOK = "gtfs"
GTFS_GOOGLE_CHAT_WEBHOOK_KEY = "google_chat_planejamento"
GTFS_MATERIALIZACAO_DATASET_ID = "gtfs"

GTFS_DBT_TEST_EXCLUDE = (
    "tecnologia_servico sumario_faixa_servico_dia sumario_faixa_servico_dia_pagamento "
    "viagem_planejada viagens_remuneradas sumario_servico_dia_historico "
    "viagem_planejada_planejamento_dia "
    "tag:freshness"
)

GTFS_DBT_TEST_SELECT = "gtfs planejamento test_consistencia_servicos_ordem_servico_gtfs"


GTFS_SELECTOR = DBTSelector(
    name="gtfs",
    initial_datetime=datetime(2000, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__gtfs",
    post_test=DBTTest(
        test_select=GTFS_DBT_TEST_SELECT,
        exclude=GTFS_DBT_TEST_EXCLUDE,
    ),
)
