# -*- coding: utf-8 -*-
"""
Valores constantes para pipeline integration__previnity_negativacao
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as common_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector
from pipelines.common.utils.gcp.bigquery import SourceTable

NEGATIVACAO_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-dunning-private",
    "dev": "rj-smtr-dev-dunning-private",
}

PREVINITY_SOURCE_NAME = "previnity"

PREVINITY_RATE_LIMIT = 300

API_URL_PF = "https://api.previnity.com.br/pnn035"
API_URL_PJ = "https://api.previnity.com.br/pnn034"

SECRET_PATH = "previnity_api"

QUERY_PF = "SELECT * FROM `rj-smtr.transito_interno.view_pessoa_fisica_negativacao`"

QUERY_PJ = "SELECT * FROM `rj-smtr.transito_interno.view_pessoa_juridica_negativacao`"

PREVINITY_SOURCES = [
    SourceTable(
        source_name=PREVINITY_SOURCE_NAME,
        table_id="retorno_negativacao",
        first_timestamp=datetime(2025, 12, 23, tzinfo=ZoneInfo(common_constants.TIMEZONE)),
        partition_date_only=True,
        bucket_names=NEGATIVACAO_PRIVATE_BUCKET_NAMES,
    )
]

NEGATIVACAO_SELECTOR = DBTSelector(
    name="autuacao_negativacao",
    initial_datetime=datetime(2025, 12, 23, tzinfo=ZoneInfo(common_constants.TIMEZONE)),
    flow_folder_name="integration__previnity_negativacao",
)
