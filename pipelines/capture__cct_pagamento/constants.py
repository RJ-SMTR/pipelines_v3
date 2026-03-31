# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de pagamento da cct
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

CCT_SOURCE_NAME = "cct"
CCT_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-cct-private", "dev": "rj-smtr-dev-private"}

CCT_SECRET_PATH = "cct_db"

CCT_HMG_SECRET_PATH = "cct_db_hmg"

CCT_TABLE_CAPTURE_PARAMS = {
    "ordem_pagamento": {
        "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento
                WHERE
                    "createdAt" BETWEEN '{start}'
                    AND '{end}'
                    OR "updatedAt" BETWEEN '{start}'
                    AND '{end}'
            """,
        "primary_keys": ["id"],
        "capture_flow": "pagamento",
    },
    "ordem_pagamento_agrupado": {
        "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento_agrupado
                WHERE
                    "createdAt" BETWEEN '{start}'
                    AND '{end}'
                    OR "updatedAt" BETWEEN '{start}'
                    AND '{end}'
            """,
        "primary_keys": ["id"],
        "capture_flow": "pagamento",
    },
    "ordem_pagamento_agrupado_historico": {
        "query": """
                SELECT
                    oph.*
                FROM
                    ordem_pagamento_agrupado_historico oph
                JOIN
                    detalhe_a da
                ON da."ordemPagamentoAgrupadoHistoricoId" = oph.id
                WHERE
                    da."createdAt" BETWEEN '{start}'
                    AND '{end}'
                    OR da."updatedAt" BETWEEN '{start}'
                    AND '{end}'
            """,
        "primary_keys": ["id"],
        "capture_flow": "pagamento",
    },
    "detalhe_a": {
        "query": """
                SELECT
                    *
                FROM
                    detalhe_a
                WHERE
                    "createdAt" BETWEEN '{start}'
                    AND '{end}'
                    OR "updatedAt" BETWEEN '{start}'
                    AND '{end}'
            """,
        "primary_keys": ["id"],
        "capture_flow": "pagamento",
    },
    "user": {
        "query": """
                SELECT
                    "id",
                    "email",
                    "provider",
                    "socialId",
                    "fullName",
                    "firstName",
                    "lastName",
                    "createdAt",
                    "updatedAt",
                    "deletedAt",
                    "permitCode",
                    "cpfCnpj",
                    "bankCode",
                    "bankAgency",
                    "bankAccount",
                    "bankAccountDigit",
                    "phone",
                    "isSgtuBlocked",
                    "passValidatorId",
                    "previousBankCode",
                    "bloqueado"
                FROM
                    public."user"
                WHERE
                    "createdAt" BETWEEN '{start}'
                    AND '{end}'
                    OR "updatedAt" BETWEEN '{start}'
                    AND '{end}'
                    OR "deletedAt" BETWEEN '{start}'
                    AND '{end}'
            """,
        "primary_keys": ["id"],
        "capture_flow": "pagamento",
    },
}


PAGAMENTO_SOURCES = [
    SourceTable(
        source_name=CCT_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2025, 10, 9, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__cct_pagamento",
        primary_keys=v["primary_keys"],
        pretreatment_reader_args=v.get("pre_treatment_reader_args"),
        pretreat_funcs=v.get("pretreat_funcs"),
        bucket_names=v.get("save_bucket_names", CCT_PRIVATE_BUCKET_NAMES),
        partition_date_only=v.get("partition_date_only", True),
        max_recaptures=v.get("max_recaptures", 4),
        raw_filetype=v.get("raw_filetype", "json"),
        file_chunk_size=v.get("file_chunk_size"),
    )
    for k, v in CCT_TABLE_CAPTURE_PARAMS.items()
    if v.get("capture_flow") == "pagamento"
]
