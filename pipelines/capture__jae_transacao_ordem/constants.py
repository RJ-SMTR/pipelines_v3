# -*- coding: utf-8 -*-
"""
Valores constantes para captura da tabela transacao_ordem
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

JAE_TRANSACAO_ORDEM_SOURCE = SourceTable(
    source_name=jae_constants.JAE_SOURCE_NAME,
    table_id=jae_constants.TRANSACAO_ORDEM_TABLE_ID,
    first_timestamp=datetime(2024, 11, 21, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__jae_transacao_ordem",
    primary_keys=["id", "id_ordem_ressarcimento", "data_processamento", "data_transacao"],
    max_recaptures=5,
)
