# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Jaé"""

from functools import partial

from prefect import task
from pytz import timezone

from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.jae import constants
from pipelines.common.capture.jae.utils import (
    get_capture_delay_minutes,
)
from pipelines.common.utils.extractors.db import get_raw_db, get_raw_db_paginated
from pipelines.common.utils.secret import get_secret


@task
def create_jae_general_extractor(context: SourceCaptureContext):
    """Cria a extração de tabelas da Jaé"""

    # if source.table_id == constants.GPS_VALIDADOR_TABLE_ID and timestamp < convert_timezone(
    #     datetime(2025, 3, 26, 15, 31, 0)
    # ):
    #     raise ValueError(
    #         """A recaptura de dados anteriores deve ser feita manualmente.
    #         A coluna de captura foi alterada de ID para data_tracking"""
    #     )

    credentials = get_secret(constants.JAE_SECRET_PATH)
    params = constants.JAE_TABLE_CAPTURE_PARAMS[context.source.table_id]

    start = context.source.get_last_scheduled_timestamp(timestamp=context.timestamp).astimezone(
        tz=timezone("UTC")
    )
    end = context.timestamp.astimezone(tz=timezone("UTC"))

    # if context.source.table_id == constants.TRANSACAO_ORDEM_TABLE_ID:
    #     start = start.replace(hour=0, minute=0, second=0)
    #     end = end.replace(hour=6, minute=0, second=0)

    start = start.strftime("%Y-%m-%d %H:%M:%S")
    end = end.strftime("%Y-%m-%d %H:%M:%S")
    capture_delay_minutes = params.get("capture_delay_minutes", {"0": 0})

    delay = get_capture_delay_minutes(
        capture_delay_minutes=capture_delay_minutes, timestamp=context.timestamp
    )

    query = params["query"].format(
        start=start,
        end=end,
        delay=delay,
    )
    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS[database_name]
    general_func_arguments = {
        "query": query,
        "engine": database["engine"],
        "host": database["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": database_name,
        "max_retries": 3,
        "raw_filepath": context.raw_filepath,
    }
    if context.source.file_chunk_size is not None:
        return partial(
            get_raw_db_paginated, page_size=context.source.file_chunk_size, **general_func_arguments
        )
    return partial(get_raw_db, **general_func_arguments)
