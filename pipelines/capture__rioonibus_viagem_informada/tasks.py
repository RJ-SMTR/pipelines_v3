# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Rio Ônibus"""

from datetime import timedelta
from functools import partial

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__rioonibus_viagem_informada import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api_list
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_viagem_informada_extractor(context: SourceCaptureContext):
    """
    Cria função extratora para dados de viagem_informada da Rio Ônibus.

    Args:
        context (SourceCaptureContext): Contexto de captura com informações de fonte e timestamp

    Returns:
        partial: Função parcial pronta para ser chamada para buscar dados da API
    """

    end_date = context.timestamp.date()
    start_date = end_date - timedelta(days=2)

    credentials = get_env_secret(constants.RIO_ONIBUS_SECRET_PATH)
    api_key = credentials["guididentificacao"]

    params = [
        {
            "guidIdentificacao": api_key,
            "datetime_processamento_inicio": d.date().isoformat() + "T00:00:00",
            "datetime_processamento_fim": d.date().isoformat() + "T23:59:59",
        }
        for d in pd.date_range(start_date, end_date)
    ]

    return partial(
        get_raw_api_list,
        url=constants.VIAGEM_INFORMADA_BASE_URL,
        params_list=params,
        raw_filepath=context.raw_filepath,
    )
