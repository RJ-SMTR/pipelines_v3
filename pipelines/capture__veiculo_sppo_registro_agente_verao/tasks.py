# -*- coding: utf-8 -*-
"""
Tasks de captura de registro de agentes de verão do SPPO
"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__veiculo_sppo_registro_agente_verao import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_sppo_agentes_verao_extractor(context: SourceCaptureContext):
    """
    Cria a extração de dados de registro de agentes de verão do SPPO.

    Args:
        context (SourceCaptureContext): Contexto de captura contendo informações da fonte

    Returns:
        callable: Função que extrai dados da API
    """

    credentials = get_env_secret(constants.SECRET_PATH)
    if not credentials:
        raise ValueError(f"Empty credentials for {constants.SECRET_PATH}")

    url = credentials.get("request_url")
    headers = {k: v for k, v in credentials.items() if k != "request_url"}

    return partial(
        get_raw_api,
        url=url,
        raw_filepath=context.raw_filepath,
        headers=headers if headers else None,
        raw_filetype="csv",
    )
