# -*- coding: utf-8 -*-
"""Constantes do pipeline de captura SONDA viagem_informada"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

SONDA_SOURCE_NAME = "sonda"
SONDA_SECRET_PATH = "sonda_api"
VIAGEM_INFORMADA_LOGIN_URL = "http://consultaviagem.m2mfrota.com.br/AutenticarUsuario"
VIAGEM_INFORMADA_BASE_URL = "https://zn4.sinopticoplus.com/servico-dados/api/v1/obterDadosGTFS"
VIAGEM_INFORMADA_TABLE_ID = "viagem_informada"

FLOW_FOLDER_NAME = "capture__sonda_viagem_informada"

VIAGEM_INFORMADA_SOURCE = SourceTable(
    source_name=SONDA_SOURCE_NAME,
    table_id=VIAGEM_INFORMADA_TABLE_ID,
    first_timestamp=datetime(2024, 9, 10, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name=FLOW_FOLDER_NAME,
    partition_date_only=True,
    max_recaptures=5,
    primary_keys=["id_viagem"],
)
