# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da SONDA"""

from datetime import timedelta
from functools import partial

import pandas as pd
import requests
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__sonda_viagem_informada import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api_list
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_viagem_informada_extractor(context: SourceCaptureContext):
    """Cria a extração de viagens informadas na api da SONDA"""

    end_date = context.timestamp.date()
    start_date = end_date - timedelta(days=2)

    login_response = requests.post(
        constants.VIAGEM_INFORMADA_LOGIN_URL,
        data=get_env_secret(constants.SONDA_SECRET_PATH),
        timeout=120,
    )
    login_response.raise_for_status()

    key = login_response.json()["IdentificacaoLogin"]

    params = [
        {"datetime_processamento": d.strftime("%d/%m/%Y 00:00:00")}
        for d in pd.date_range(start_date, end_date)
    ]

    headers = {"Authorization": key}

    return partial(
        get_raw_api_list,
        url=constants.VIAGEM_INFORMADA_BASE_URL,
        params_list=params,
        headers=headers,
        raw_filepath=context.raw_filepath,
    )
