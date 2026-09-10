# -*- coding: utf-8 -*-
"""Tasks para captura dos dados das planilhas de controle_financeiro"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__smtr_controle_financeiro import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api


@task(cache_policy=NO_CACHE)
def create_controle_financeiro_extractor(context: SourceCaptureContext):
    """
    Cria função de extração para as planilhas de controle financeiro.

    Args:
        context: Contexto da captura contendo informações do source e timestamp

    Returns:
        Callable: Função parcial configurada para extração
    """
    url = (
        constants.SHEETS_BASE_URL
        + constants.SHEETS_CAPTURE_PARAMS[context.source.table_id]["sheet_id"]
    )

    return partial(
        get_raw_api,
        url=url,
        raw_filepath=context.raw_filepath,
        raw_filetype=context.source.raw_filetype,
    )
