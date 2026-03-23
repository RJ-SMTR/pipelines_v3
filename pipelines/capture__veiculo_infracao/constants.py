# -*- coding: utf-8 -*-
"""
Valores constantes para captura de infrações de veículos SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

# Table IDs
SPPO_INFRACAO_TABLE_ID = "infracao"

# Column mappings from raw data
SPPO_INFRACAO_MAPPING_KEYS = {
    "permissao": "permissao",
    "modal": "modo",
    "placa": "placa",
    "cm": "id_auto_infracao",
    "data_infracao": "data_infracao",
    "valor": "valor",
    "cod_infracao": "id_infracao",
    "des_infracao": "infracao",
    "status": "status",
    "data_pagamento": "data_pagamento",
    "linha": "servico",
}

# CSV reading arguments
SPPO_INFRACAO_CSV_ARGS = {
    "sep": ";",
    "names": SPPO_INFRACAO_MAPPING_KEYS.keys(),
}

# FTP path for raw data
SPPO_INFRACAO_FTP_PATH = "MULTAS/MULTAS"

# RDO FTPS credentials secret path
RDO_FTPS_SECRET_PATH = "rdo_ftps"

# Dataset and table configuration
SPPO_INFRACAO_SOURCES = [
    SourceTable(
        source_name="RDO FTPS",
        table_id=SPPO_INFRACAO_TABLE_ID,
        first_timestamp=datetime(
            2026, 3, 23, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)
        ),
        flow_folder_name="capture__veiculo_infracao",
        primary_keys=["id_auto_infracao"],
        pretreatment_reader_args=SPPO_INFRACAO_CSV_ARGS,
        raw_filetype="txt",
        partition_date_only=True,
    )
]
