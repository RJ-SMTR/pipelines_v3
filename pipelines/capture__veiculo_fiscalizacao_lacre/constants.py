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
VEICULO_LACRE_SHEET_ID = "1LTyNe2_AgWR0JlCmUOYGtYKpe33w57hslkMkrUqYPbw"
VEICULO_LACRE_SHEET_NAME = "Controle de processos - Fiscalização"

VEICULO_LACRE_RENAME_MAPPING = {
    "motivo": "motivo",
    "processo": "processo",
    "consorcio": "consorcio",
    "permissionario_empresa": "permissionario_empresa",
    "placa": "placa",
    "n_de_ordem": "n_o_de_ordem",
    "n_do_auto": "no_do_auto",
    "n_do_lacre": "no_do_lacre",
    "data_do_lacre": "data_do_lacre",
    "data_do_deslacre": "data_do_deslacre",
    "nome_do_fiscal_deslacre": "nome_do_fiscal_deslacre",
    "dias_lacrados": "dias_lacrados",
    "motivo_do_lacre": "motivo_do_lacre",
    "ultimo_editor": "ultimo_editor",
    "ultima_atualizacao": "ultima_atualizacao",
}

VEICULO_LACRE_SOURCE = SourceTable(
    source_name=VEICULO_FISCALIZACAO_SOURCE_NAME,
    table_id=VEICULO_LACRE_TABLE_ID,
    first_timestamp=datetime(2025, 5, 28, 5, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__veiculo_fiscalizacao_lacre",
    partition_date_only=True,
    max_recaptures=5,
    primary_keys=["placa", "n_o_de_ordem", "data_do_lacre"],
    raw_filetype="csv",
)
