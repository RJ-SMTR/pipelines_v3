# -*- coding: utf-8 -*-
"""
Valores constantes para captura de registro de agentes de verão do SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.veiculo import constants as veiculo_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

TABLE_ID = "sppo_registro_agente_verao"
SECRET_PATH = "smtr_agentes_verao"

SPPO_REGISTRO_AGENTE_VERAO_COLUMNS = [
    "datetime_registro",
    "email",
    "id_veiculo",
    "servico",
    "link_foto",
    "validacao",
]

SPPO_REGISTRO_AGENTE_VERAO_SOURCES = [
    SourceTable(
        source_name=veiculo_constants.SPPO_VEICULO_SOURCE_NAME,
        table_id=TABLE_ID,
        first_timestamp=datetime(2026, 4, 17, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        flow_folder_name="capture__veiculo_sppo_registro_agente_verao",
        primary_keys=["datetime_registro", "email"],
        pretreatment_reader_args={
            "skiprows": 2,
            "names": SPPO_REGISTRO_AGENTE_VERAO_COLUMNS,
        },
        partition_date_only=True,
        max_recaptures=4,
        raw_filetype="csv",
    )
]
