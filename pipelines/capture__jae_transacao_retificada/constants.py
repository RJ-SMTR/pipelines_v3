# -*- coding: utf-8 -*-
"""
Valores constantes para captura da tabela transacao_retificada
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

TRANSACAO_RETIFICADA_SOURCE = SourceTable(
    source_name=jae_constants.JAE_SOURCE_NAME,
    table_id=jae_constants.TRANSACAO_RETIFICADA_TABLE_ID,
    first_timestamp=datetime(2025, 6, 3, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__jae_transacao_retificada",
    primary_keys=["id"],
    bucket_names=jae_constants.JAE_PRIVATE_BUCKET_NAMES,
    max_recaptures=60,
)
