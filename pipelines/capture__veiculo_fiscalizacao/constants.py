# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de fiscalização de veículos
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

VEICULO_FISCALIZACAO_SOURCE_NAME = "veiculo_fiscalizacao"
VEICULO_LACRE_TABLE_ID = "veiculo_fiscalizacao_lacre"

VEICULO_LACRE_SOURCE = SourceTable(
    source_name=VEICULO_FISCALIZACAO_SOURCE_NAME,
    table_id=VEICULO_LACRE_TABLE_ID,
    first_timestamp=datetime(2025, 5, 28, 5, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    partition_date_only=True,
    max_recaptures=5,
    primary_keys=["placa", "n_o_de_ordem", "data_do_lacre"],
    raw_filetype="csv",
)
