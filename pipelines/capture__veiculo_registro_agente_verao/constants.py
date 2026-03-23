# -*- coding: utf-8 -*-
"""
Valores constantes para captura de registro de agentes de verão de veículos SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

# Table ID
SPPO_REGISTRO_AGENTE_VERAO_TABLE_ID = "sppo_registro_agente_verao"

# Column names for CSV
SPPO_REGISTRO_AGENTE_VERAO_COLUMNS = [
    "datetime_registro",
    "email",
    "id_veiculo",
    "servico",
    "link_foto",
    "validacao",
]

# Secret path for API credentials
AGENTES_VERAO_SECRET_PATH = "smtr_agentes_verao"

# Dataset and table configuration
SPPO_REGISTRO_AGENTE_VERAO_SOURCES = [
    SourceTable(
        source_name="API Agentes Verão",
        table_id=SPPO_REGISTRO_AGENTE_VERAO_TABLE_ID,
        first_timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        flow_folder_name="capture__veiculo_registro_agente_verao",
        primary_keys=["datetime_registro", "email"],
        raw_filetype="csv",
        partition_date_only=True,
        pretreatment_reader_args={
            "skiprows": 2,
            "names": SPPO_REGISTRO_AGENTE_VERAO_COLUMNS,
        },
    )
]
