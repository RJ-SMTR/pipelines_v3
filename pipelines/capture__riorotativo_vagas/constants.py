# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de vagas do Rio Rotativo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

RIOROTATIVO_SOURCE_NAME = "riorotativo"
RIOROTATIVO_VAGAS_SPREADSHEET_ID = "1z0KA7kMjsq6visGOt6HTk1jkotKOSCVASp0BxCdt3cM"
RIOROTATIVO_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-riorotativo-private",
    "dev": "rj-smtr-dev-private",
}

RIOROTATIVO_VAGAS_TABLE_CAPTURE_PARAMS = {
    "area_estacionamento": {
        "sheet_name": "area_estacionamento",
        "primary_keys": ["area_codigo"],
    },
    "perfil_funcionamento": {
        "sheet_name": "perfil_funcionamento",
        "primary_keys": ["perfil_funcionamento_codigo"],
    },
    "perfil_funcionamento_excecao": {
        "sheet_name": "perfil_funcionamento_excecao",
        "primary_keys": ["area_codigo", "perfil_funcionamento_codigo"],
    },
}

RIOROTATIVO_VAGAS_SOURCES = [
    SourceTable(
        source_name=RIOROTATIVO_SOURCE_NAME,
        table_id=k,
        first_timestamp=v.get(
            "first_timestamp",
            datetime(2026, 7, 6, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        ),
        flow_folder_name="capture__riorotativo_vagas",
        primary_keys=v["primary_keys"],
        pretreatment_reader_args=v.get("pretreatment_reader_args"),
        pretreat_funcs=v.get("pretreat_funcs"),
        bucket_names=RIOROTATIVO_PRIVATE_BUCKET_NAMES,
        partition_date_only=v.get("partition_date_only", True),
        max_recaptures=v.get("max_recaptures", 4),
        raw_filetype="csv",
    )
    for k, v in RIOROTATIVO_VAGAS_TABLE_CAPTURE_PARAMS.items()
]

RIOROTATIVO_VAGAS_EXTRA_PARAMETERS = {
    table_id: {
        "spread_sheet_id": params.get(
            "spread_sheet_id",
            RIOROTATIVO_VAGAS_SPREADSHEET_ID,
        ),
        "sheet_name": params["sheet_name"],
        "filter_expr": params.get("filter_expr"),
        "rename_mapping": params.get("rename_mapping"),
        "dtypes": params.get("dtypes"),
        "parse_dates": params.get("parse_dates"),
    }
    for table_id, params in RIOROTATIVO_VAGAS_TABLE_CAPTURE_PARAMS.items()
}
