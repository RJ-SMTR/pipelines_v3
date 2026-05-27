# -*- coding: utf-8 -*-
"""Tasks para os processos manuais de ordem atrasada da bilhetagem"""

from datetime import datetime

from prefect import task

from pipelines.capture__jae_integracao import constants as integracao_constants
from pipelines.capture__jae_transacao_ordem import constants as transacao_constants
from pipelines.common.capture.jae import constants as jae_constants


@task
def create_transacao_ordem_integracao_capture_params(timestamp: datetime, table_id: str) -> dict:
    source_map = {
        jae_constants.TRANSACAO_ORDEM_TABLE_ID: transacao_constants.JAE_TRANSACAO_ORDEM_SOURCE,
        jae_constants.INTEGRACAO_TABLE_ID: integracao_constants.INTEGRACAO_SOURCE,
    }
    return {
        "timestamp": source_map[table_id]
        .get_last_scheduled_timestamp(timestamp=timestamp)
        .strftime("%Y-%m-%d %H:%M:%S"),
        "recapture": False,
    }
