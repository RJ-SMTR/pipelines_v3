# -*- coding: utf-8 -*-
"""
Tasks para captura de dados de ordem de pagamento da Jaé
"""

from datetime import timedelta
from functools import partial

from prefect import task
from pytz import timezone

from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.jae import constants
from pipelines.common.capture.jae.utils import (
    get_capture_delay_minutes,
)
from pipelines.common.utils.extractors.db import get_raw_db, get_raw_db_paginated
from pipelines.common.utils.secret import get_env_secret


@task
def create_ressarcimento_db_extractor(context: SourceCaptureContext):
    """Cria a extração de tabelas do ressarcimento_db da Jaé"""
    credentials = get_env_secret(constants.JAE_SECRET_PATH.value)
    params = constants.JAE_TABLE_CAPTURE_PARAMS.value[context.source.table_id]

    end = context.timestamp.astimezone(tz=timezone("UTC"))
    start = (end - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
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
    database = constants.JAE_DATABASE_SETTINGS.value[database_name]
    general_func_arguments = {
        "query": query,
        "engine": database["engine"],
        "host": database["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": database_name,
        "max_retries": 3,
    }
    if context.source.file_chunk_size is not None:
        return partial(
            get_raw_db_paginated, page_size=context.source.file_chunk_size, **general_func_arguments
        )
    return partial(get_raw_db, **general_func_arguments)
