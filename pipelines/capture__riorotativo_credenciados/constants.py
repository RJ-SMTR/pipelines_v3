# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de credenciados do Rio Rotativo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

RIOROTATIVO_SOURCE_NAME = "riorotativo"
RIOROTATIVO_CREDENCIADOS_SPREADSHEET_ID = "1pAQ59MuY9cLgYfc3pbyg1hH7RCBy1nDDZCSfVAd2DIw"
RIOROTATIVO_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-riorotativo-private",
    "dev": "rj-smtr-dev-private",
}

RIOROTATIVO_RENAME_MAPPING = {
    "motivobloqueio": "motivo_bloqueio",
    "decisaobloqueio": "decisao_bloqueio",
    "datainiciobloqueio": "data_inicio_bloqueio",
    "datafimbloqueio": "data_fim_bloqueio",
    "ultimoeditor": "ultimo_editor",
    "ultimaatualizacao": "ultima_atualizacao",
}

RIOROTATIVO_CREDENCIADOS_TABLE_CAPTURE_PARAMS = {
    "entidade_42498733000148": {
        "sheet_name": "42498733000148",
        "dtypes": str,
        "primary_keys": ["cpf"],
    },
    "lista_bloqueio": {
        "sheet_name": "lista_bloqueio",
        "primary_keys": ["cpf"],
    },
}

RIOROTATIVO_CREDENCIADOS_SOURCES = [
    SourceTable(
        source_name=RIOROTATIVO_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2026, 7, 6, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__riorotativo_credenciados",
        primary_keys=v["primary_keys"],
        pretreatment_reader_args=v.get("pretreatment_reader_args"),
        pretreat_funcs=v.get("pretreat_funcs"),
        bucket_names=RIOROTATIVO_PRIVATE_BUCKET_NAMES,
        partition_date_only=v.get("partition_date_only", True),
        max_recaptures=v.get("max_recaptures", 4),
        raw_filetype="csv",
    )
    for k, v in RIOROTATIVO_CREDENCIADOS_TABLE_CAPTURE_PARAMS.items()
]

RIOROTATIVO_CREDENCIADOS_EXTRA_PARAMETERS = {
    table_id: {
        "spread_sheet_id": params.get(
            "spread_sheet_id",
            RIOROTATIVO_CREDENCIADOS_SPREADSHEET_ID,
        ),
        "sheet_name": params["sheet_name"],
        "filter_expr": params.get("filter_expr"),
        "rename_mapping": params.get("rename_mapping", RIOROTATIVO_RENAME_MAPPING),
        "dtypes": params.get("dtypes"),
        "parse_dates": params.get("parse_dates"),
    }
    for table_id, params in RIOROTATIVO_CREDENCIADOS_TABLE_CAPTURE_PARAMS.items()
}
