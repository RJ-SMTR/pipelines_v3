# -*- coding: utf-8 -*-
"""
Valores constantes para captura da tabela transacao_erro
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

JAE_TRANSACAO_ERRO_SOURCE = SourceTable(
    source_name=jae_constants.JAE_SOURCE_NAME,
    table_id=jae_constants.TRANSACAO_ERRO_TABLE_ID,
    first_timestamp=datetime(2026, 1, 26, 23, 1, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__jae_transacao_erro",
    primary_keys=["id_transacao_recebida"],
    bucket_names=jae_constants.JAE_PRIVATE_BUCKET_NAMES,
    max_recaptures=60,
)
