# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de temperatura do INMET"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__inmet_temperatura import constants
from pipelines.capture__inmet_temperatura.utils import (
    get_inmet_capture_window,
    split_date_range,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api_list
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_temperatura_extractor(context: SourceCaptureContext):
    """Cria a extração de dados da api do INMET"""

    start_date, end_date = get_inmet_capture_window(timestamp=context.timestamp)
    date_ranges = split_date_range(start_date=start_date, end_date=end_date)

    key = get_env_secret(constants.INMET_SECRET_PATH)["key"]

    url_list = []
    for estacao in constants.INMET_ESTACOES:
        for range_start, range_end in date_ranges:
            url_list.append(
                f"{constants.INMET_BASE_URL}/{range_start:%Y-%m-%d}/"
                f"{range_end:%Y-%m-%d}/{estacao}/{key}"
            )

    return partial(get_raw_api_list, url=url_list, raw_filepath=context.raw_filepath)
