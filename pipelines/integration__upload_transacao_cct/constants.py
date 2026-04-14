# -*- coding: utf-8 -*-
"""
Valores constantes para exportação das transações do BQ para o Postgres
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

TRANSACAO_POSTGRES_TABLE_NAME = "transacao_bigquery"
TRANSACAO_POSTGRES_TMP_TABLE_NAME = f"tmp__{TRANSACAO_POSTGRES_TABLE_NAME}"
REDIS_KEY = "cct_upload_transacao_bigquery"
LAST_UPLOAD_TIMESTAMP_KEY_NAME = "last_upload_timestamp"
TRANSACAO_CCT_FOLDER = "transacao_cct"
EXPORT_GCS_PREFIX = f"upload/{TRANSACAO_CCT_FOLDER}"
PROJETO_APP_CCT_DATASET_ID = "projeto_app_cct"
TRANSACAO_CCT_VIEW_FULL_NAME = f"{PROJETO_APP_CCT_DATASET_ID}.transacao_cct"
TMP_TABLE_INDEX_NAME = "idx_tmp_id_transacao"
FINAL_TABLE_ID_TRANSACAO_INDEX_NAME = "idx_transacao_id_transacao"
FINAL_TABLE_ID_ORDEM_INDEX_NAME = "idx_transacao_id_ordem"
FINAL_TABLE_DATA_INDEX_NAME = "idx_transacao_data"
LOG_TABLE_NAME = "transacao_bigquery_data_alteracao"
LOG_TRIGGER_NAME = "trg_transacao_bigquery_data_alteracao"
LOG_FUNCTION_NAME = "log_transacao_bigquery_data_alteracao"
TESTE_SINCRONIZACAO_TABLE_NAME = "teste_sincronizacao_transacao_cct"

TESTE_SINCRONIZACAO_POST_TEST = DBTTest(
    test_select=TESTE_SINCRONIZACAO_TABLE_NAME,
    test_descriptions={
        TESTE_SINCRONIZACAO_TABLE_NAME: {
            "dbt_utils.expression_is_true__teste_sincronizacao_transacao_cct": {
                "description": "Todas as datas estão com os dados sincronizados"
            },
        }
    },
)

TESTE_SINCRONIZACAO_SELECTOR = DBTSelector(
    name="teste_sincronizacao_transacao_cct",
    initial_datetime=datetime(2026, 4, 8, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    post_test=TESTE_SINCRONIZACAO_POST_TEST,
)
