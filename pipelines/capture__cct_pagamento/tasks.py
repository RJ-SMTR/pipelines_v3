# -*- coding: utf-8 -*-
"""
Tasks para captura de dados de pagamento da cct
"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE
from pytz import timezone

from pipelines.capture__cct_pagamento import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.db import get_raw_db
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_cct_general_extractor(context: SourceCaptureContext):
    """Cria a extração de tabelas da CCT"""

    credentials = get_env_secret(constants.CCT_SECRET_PATH)

    params = constants.CCT_TABLE_CAPTURE_PARAMS[context.source.table_id]

    start = (
        context.source.get_last_scheduled_timestamp(timestamp=context.timestamp)
        .astimezone(tz=timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    end = context.timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    query = params["query"].format(
        start=start,
        end=end,
    )

    general_func_arguments = {
        "query": query,
        "engine": "postgresql",
        "host": credentials["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": credentials["dbname"],
        "max_retries": 3,
        "raw_filepath": context.raw_filepath,
    }

    return partial(get_raw_db, **general_func_arguments)
