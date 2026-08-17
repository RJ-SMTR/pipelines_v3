# -*- coding: utf-8 -*-
"""Tasks de captura das viagens informadas da Maxtrack."""

from datetime import timedelta
from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__maxtrack_viagem_informada import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api_paginated
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_viagem_informada_extractor(context: SourceCaptureContext):
    """Cria a função extratora de viagens informadas da Maxtrack."""
    end_date = context.timestamp.date()
    start_date = end_date - timedelta(days=1)
    credentials = get_env_secret(constants.MAXTRACK_SECRET_PATH)
    if "guid" not in credentials:
        raise KeyError(f"Credential 'guid' not found in {constants.MAXTRACK_SECRET_PATH}")

    params = {
        "guid": credentials["guid"],
        "startDate": f"{start_date.isoformat()}T00:00:00Z",
        "endDate": f"{end_date.isoformat()}T23:59:59Z",
    }

    return partial(
        get_raw_api_paginated,
        url=constants.VIAGEM_INFORMADA_BASE_URL,
        raw_filepath=context.raw_filepath,
        params=params,
        page_param_name="page",
        page_size_param_name="pageSize",
        page_size=constants.MAXTRACK_PAGE_SIZE,
        response_key="data",
        first_page=1,
    )
