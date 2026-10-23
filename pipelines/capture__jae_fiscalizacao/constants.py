# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de estacionamento_veiculo do Rio Rotativo Digital da Jaé
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.utils.gcp.bigquery import SourceTable

FISCALIZACAO_SOURCE = SourceTable(
    source_name=jae_constants.JAE_SOURCE_NAME,
    table_id=jae_constants.FISCALIZACAO_TABLE_ID,
    first_timestamp=datetime(2026, 10, 21, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="capture__jae_fiscalizacao",
    primary_keys=["id"],
    bucket_names=jae_constants.JAE_PRIVATE_BUCKET_NAMES,
    file_chunk_size=200_000,
)
