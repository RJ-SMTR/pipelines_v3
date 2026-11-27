# -*- coding: utf-8 -*-
"""
Valores constantes para exportação das transações do BQ para o Postgres
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.integration__upload_transacao_cct.utils import TableUploadConfig

LAST_UPLOAD_TIMESTAMP_KEY_NAME = "last_upload_timestamp"
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

_TRANSACAO_CCT_POST_TEST = DBTTest(
    test_select=TESTE_SINCRONIZACAO_TABLE_NAME,
    test_descriptions={
        TESTE_SINCRONIZACAO_TABLE_NAME: {
            "dbt_utils__expression_is_true__teste_sincronizacao_transacao_cct": {
                "description": "Todas as datas estão com os dados sincronizados"
            },
        }
    },
)

_TRANSACAO_CCT_SELECTOR = DBTSelector(
    name="teste_sincronizacao_transacao_cct",
    initial_datetime=datetime(2026, 4, 8, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    post_test=_TRANSACAO_CCT_POST_TEST,
)

TRANSACAO_CCT_CONFIG = TableUploadConfig(
    table_name="transacao_bigquery",
    tmp_table_name="tmp__transacao_bigquery",
    tmp_table_ddl="""
        id_transacao character varying(60),
        data date,
        datetime_transacao timestamp,
        consorcio character varying(20),
        tipo_transacao character varying(50),
        valor_pagamento numeric(13,5),
        data_ordem date,
        id_ordem_pagamento integer,
        id_ordem_pagamento_consorcio_operador_dia integer,
        datetime_ultima_atualizacao timestamp,
        datetime_export timestamp
    """,
    tmp_index_name="idx_tmp_id_transacao",
    tmp_index_columns="id_transacao",
    unique_key_fields=["id_transacao"],
    id_transacao_index_name="idx_transacao_id_transacao",
    id_ordem_index_name="idx_transacao_id_ordem",
    data_index_name="idx_transacao_data",
    log_table_name="transacao_bigquery_data_alteracao",
    log_trigger_name="trg_transacao_bigquery_data_alteracao",
    log_function_name="log_transacao_bigquery_data_alteracao",
    redis_key="cct_upload_transacao_bigquery",
    folder="transacao_cct",
    gcs_prefix="upload/transacao_cct",
    view_full_name=f"{PROJETO_APP_CCT_DATASET_ID}.transacao_cct",
    bq_source_table="transacao",
    partition_field="data",
    use_data_ordem_transacao_join=True,
    teste_sincronizacao_table_name="teste_sincronizacao_transacao_cct",
    teste_sincronizacao_selector=_TRANSACAO_CCT_SELECTOR,
)

_TRANSACAO_VALOR_ORDEM_POST_TEST = DBTTest(
    test_select="teste_sincronizacao_transacao_valor_ordem_cct",
    test_descriptions={
        "teste_sincronizacao_transacao_valor_ordem_cct": {
            "dbt_utils__expression_is_true__teste_sincronizacao_transacao_valor_ordem_cct": {
                "description": "Todas as datas estão com os dados sincronizados"
            },
        }
    },
)

_TRANSACAO_VALOR_ORDEM_SELECTOR = DBTSelector(
    name="teste_sincronizacao_transacao_valor_ordem_cct",
    initial_datetime=datetime(2026, 10, 2, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    post_test=_TRANSACAO_VALOR_ORDEM_POST_TEST,
)

TRANSACAO_VALOR_ORDEM_CONFIG = TableUploadConfig(
    table_name="transacao_valor_ordem",
    tmp_table_name="tmp__transacao_valor_ordem",
    tmp_table_ddl="""
        data_ordem date,
        data_transacao date,
        id_transacao character varying(60),
        id_operadora character varying(60),
        valor_transacao_rateio numeric(13,5),
        id_ordem_pagamento integer,
        id_ordem_pagamento_consorcio_dia integer,
        id_ordem_pagamento_consorcio_operador_dia integer,
        datetime_ultima_atualizacao timestamp,
        datetime_export timestamp
    """,
    tmp_index_name="idx_tmp_id_transacao_valor_ordem",
    tmp_index_columns="id_transacao, data_ordem",
    unique_key_fields=["id_transacao", "data_ordem"],
    id_transacao_index_name="idx_transacao_valor_ordem_id_transacao",
    id_ordem_index_name="idx_transacao_valor_ordem_id_ordem",
    data_index_name="idx_transacao_valor_ordem_data_ordem",
    log_table_name="transacao_valor_ordem_data_alteracao",
    log_trigger_name="trg_transacao_valor_ordem_data_alteracao",
    log_function_name="log_transacao_valor_ordem_data_alteracao",
    redis_key="cct_upload_transacao_valor_ordem",
    folder="transacao_valor_ordem_cct",
    gcs_prefix="upload/transacao_valor_ordem_cct",
    view_full_name=f"{PROJETO_APP_CCT_DATASET_ID}.transacao_valor_ordem_cct",
    bq_source_table="transacao_valor_ordem",
    partition_field="data_ordem",
    use_data_ordem_transacao_join=False,
    teste_sincronizacao_table_name="teste_sincronizacao_transacao_valor_ordem_cct",
    teste_sincronizacao_selector=_TRANSACAO_VALOR_ORDEM_SELECTOR,
)

TABLE_UPLOAD_CONFIGS: dict[str, TableUploadConfig] = {
    "transacao_cct": TRANSACAO_CCT_CONFIG,
    "transacao_valor_ordem_cct": TRANSACAO_VALOR_ORDEM_CONFIG,
}
