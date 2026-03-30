# -*- coding: utf-8 -*-
"""
Tasks para o flow de captura de dados do STU.
"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__stu_tabelas.utils import extract_stu_data
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


@task(cache_policy=NO_CACHE)
def create_stu_extractor(context: SourceCaptureContext):
    """
    Cria a extração de dados do STU.

    Args:
        context: Contexto da captura contendo informações do source e timestamp

    Returns:
        partial: Função parcial configurada para extração dos dados
    """

    return partial(
        extract_stu_data,
        source=context.source,
        timestamp=context.timestamp,
    )
