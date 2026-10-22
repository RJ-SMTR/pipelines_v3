# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Rio Ônibus"""

from datetime import timedelta, timezone
from functools import partial

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

    end_datetime = context.timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    end_datetime = end_datetime.astimezone(timezone.utc)
    start_datetime = end_datetime - timedelta(days=1)

    credentials = get_env_secret(constants.RIO_ONIBUS_SECRET_PATH)
    api_key = credentials["guididentificacao"]

    # Dia anterior em America/Sao_Paulo, enviado em UTC sem fuso; o fim é exclusivo (API v1.1)
    params = {
        "guidIdentificacao": api_key,
        "datetime_processamento_inicio": start_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
        "datetime_processamento_fim": end_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    return partial(
        get_raw_api_list,
        url=constants.VIAGEM_INFORMADA_BASE_URL,
        params_list=[params],
        raw_filepath=context.raw_filepath,
        timeout=600,
    )
