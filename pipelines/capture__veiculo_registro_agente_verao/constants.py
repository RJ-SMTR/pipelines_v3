# -*- coding: utf-8 -*-
"""
Valores constantes para captura de registro de agentes de verão de veículos SPPO
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

# Source configuration
SPPO_REGISTRO_AGENTE_VERAO_SOURCE_NAME = "API Agentes Verão"

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

# CSV reading arguments
SPPO_REGISTRO_AGENTE_VERAO_CSV_ARGS = {
    "skiprows": 2,
    "names": SPPO_REGISTRO_AGENTE_VERAO_COLUMNS,
}


def pretreat_registro_agente_verao(
    data: pd.DataFrame,
    timestamp=None,  # noqa: ARG001
    primary_keys=None,  # noqa: ARG001
) -> pd.DataFrame:
    """
    Aplica pré-tratamento aos dados de registros de agentes de verão.

    Nenhuma transformação necessária pois as colunas já possuem os nomes corretos.

    Args:
        data (pd.DataFrame): Dataframe com os dados brutos
        timestamp: Timestamp da captura (não utilizado)
        primary_keys: Primary keys da tabela (não utilizadas)

    Returns:
        pd.DataFrame: Dataframe original
    """
    return data


# Dataset and table configuration
SPPO_REGISTRO_AGENTE_VERAO_SOURCES = [
    SourceTable(
        source_name=SPPO_REGISTRO_AGENTE_VERAO_SOURCE_NAME,
        table_id=SPPO_REGISTRO_AGENTE_VERAO_TABLE_ID,
        first_timestamp=datetime(2024, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        flow_folder_name="capture__veiculo_registro_agente_verao",
        primary_keys=["datetime_registro", "email"],
        pretreatment_reader_args=SPPO_REGISTRO_AGENTE_VERAO_CSV_ARGS,
        pretreat_funcs=[pretreat_registro_agente_verao],
        raw_filetype="csv",
        partition_date_only=True,
    )
]
