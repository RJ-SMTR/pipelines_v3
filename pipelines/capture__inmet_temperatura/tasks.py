# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de temperatura do INMET"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__inmet_temperatura import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api_list
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_temperatura_extractor(context: SourceCaptureContext):
    """Cria a extração de dados da api do INMET"""

    capture_date = context.timestamp.strftime("%Y-%m-%d")

    key = get_env_secret(constants.INMET_SECRET_PATH)["key"]

    url_list = []
    for estacao in constants.INMET_ESTACOES:
        url_list.append(f"{constants.INMET_BASE_URL}/{capture_date}/{capture_date}/{estacao}/{key}")

    return partial(get_raw_api_list, url=url_list, raw_filepath=context.raw_filepath)
