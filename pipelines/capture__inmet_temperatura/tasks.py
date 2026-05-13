# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de temperatura do INMET"""

from datetime import timedelta
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

    start = context.timestamp - timedelta(days=1)
    data_inicio = start.strftime("%Y-%m-%d")
    data_fim = context.timestamp.strftime("%Y-%m-%d")

    key = get_env_secret(constants.INMET_SECRET_PATH)

    url_list = []
    for estacao in constants.INMET_ESTACOES:
        url = f"{constants.INMET_BASE_URL}/{data_inicio}/{data_fim}/{estacao}/{key}"
        url_list.append(url)

    return partial(get_raw_api_list, url=url_list, raw_filepath=context.raw_filepath)
