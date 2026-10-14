# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do flow treatment__subsidio_sppo_apuracao
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

TZ = ZoneInfo(smtr_constants.TIMEZONE)

DATA_SUBSIDIO_V9_INICIO = datetime(2024, 8, 16, 7, 5, 0, tzinfo=TZ)
DATA_SUBSIDIO_V14_INICIO = datetime(2025, 1, 5, 7, 5, 0, tzinfo=TZ)
DATA_SUBSIDIO_V25_INICIO = datetime(2026, 8, 1, 7, 5, 0, tzinfo=TZ)
SUBSIDIO_INITIAL_DATETIME = datetime(2022, 6, 1, 7, 5, 0, tzinfo=TZ)

INCREMENTAL_DELAY_HOURS = 24 * 7  # D-7

TABLE_IDS_JAE = ["transacao", "transacao_riocard", "gps_validador"]

WEBHOOK_KEY = "subsidio_data_check"

ADDITIONAL_VARS = {"tipo_teste": "subsidio"}

PRE_TEST_SELECT = (
    "sppo_registros sppo_realocacao check_gps_treatment__gps_sppo sppo_veiculo_dia"
    " veiculo_dia tecnologia_servico viagem_planejada transacao transacao_riocard"
    " gps_validador test_completude__temperatura test_jae_captura_subsidio viagem_completa"
)
PRE_TEST_V25_SELECT = (
    "veiculo_dia tecnologia_servico servico_planejado_faixa_horaria transacao transacao_riocard"
    " gps_validador test_completude__temperatura test_jae_captura_subsidio viagem_valida"
)
PRE_TEST_EXCLUDE = (
    "dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas"
    " not_null__data_ordem__transacao"
    " sincronizacao_tabelas__transacao_gratuidade_estudante_municipal"
    " dbt_expectations__expect_row_values_to_have_recent_data__datetime_captura__gps_validador"
)


POST_TEST_V8_SELECT = "dashboard_subsidio_sppo"
POST_TEST_V9_SELECT = "viagens_remuneradas sumario_servico_dia_pagamento valor_km_tipo_viagem"
POST_TEST_V14_SELECT = (
    "viagem_classificada viagem_regularidade_temperatura viagens_remuneradas"
    " sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem"
)


APURACAO_SUBSIDIO_PRE_TEST = DBTTest(
    test_select=PRE_TEST_SELECT,
    exclude=PRE_TEST_EXCLUDE,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V25_PRE_TEST = DBTTest(
    test_select=PRE_TEST_V25_SELECT,
    exclude=PRE_TEST_EXCLUDE,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V8_POST_TEST = DBTTest(
    test_select=POST_TEST_V8_SELECT,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V9_POST_TEST = DBTTest(
    test_select=POST_TEST_V9_SELECT,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)

APURACAO_SUBSIDIO_V14_POST_TEST = DBTTest(
    test_select=POST_TEST_V14_SELECT,
    additional_vars=ADDITIONAL_VARS,
    truncate_date=True,
)


APURACAO_SUBSIDIO_V8_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v8",
    initial_datetime=SUBSIDIO_INITIAL_DATETIME,
    final_datetime=DATA_SUBSIDIO_V9_INICIO - timedelta(days=1),
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    post_test=APURACAO_SUBSIDIO_V8_POST_TEST,
)

APURACAO_SUBSIDIO_V9_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v9",
    initial_datetime=DATA_SUBSIDIO_V9_INICIO,
    final_datetime=DATA_SUBSIDIO_V14_INICIO - timedelta(days=1),
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    post_test=APURACAO_SUBSIDIO_V9_POST_TEST,
)

APURACAO_SUBSIDIO_V14_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v14",
    initial_datetime=DATA_SUBSIDIO_V14_INICIO,
    final_datetime=DATA_SUBSIDIO_V25_INICIO - timedelta(days=1),
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    pre_test=APURACAO_SUBSIDIO_PRE_TEST,
    post_test=APURACAO_SUBSIDIO_V14_POST_TEST,
)

APURACAO_SUBSIDIO_V25_SELECTOR = DBTSelector(
    name="apuracao_subsidio_v14",
    initial_datetime=DATA_SUBSIDIO_V25_INICIO,
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
    pre_test=APURACAO_SUBSIDIO_V25_PRE_TEST,
    post_test=APURACAO_SUBSIDIO_V14_POST_TEST,
)

MONITORAMENTO_SUBSIDIO_SELECTOR = DBTSelector(
    name="monitoramento_subsidio",
    initial_datetime=DATA_SUBSIDIO_V9_INICIO,
    incremental_delay_hours=INCREMENTAL_DELAY_HOURS,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)

SNAPSHOT_SUBSIDIO_SELECTOR = DBTSelector(
    name="snapshot_subsidio",
    initial_datetime=DATA_SUBSIDIO_V9_INICIO,
    flow_folder_name="treatment__subsidio_sppo_apuracao",
)
