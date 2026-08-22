# -*- coding: utf-8 -*-
"""Tasks de captura das viagens informadas da Maxtrack."""

from datetime import timedelta
from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__maxtrack_viagem_informada import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.api import get_raw_api
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_viagem_informada_extractor(context: SourceCaptureContext):
    """Cria a função extratora de viagens informadas da Maxtrack."""
    end_date = context.timestamp.date()
    start_date = end_date - timedelta(days=1)
    token = get_env_secret(
        secret_path=constants.MAXTRACK_SECRET_PATH,
        secret_name="token",
    )["token"]

    params = {
        "datetime_processamento_inicio": f"{start_date.isoformat()}T00:00:00Z",
        "datetime_processamento_fim": f"{end_date.isoformat()}T23:59:59Z",
    }
    headers = {"Authorization": f"Bearer {token}"}

    return partial(
        get_raw_api,
        url=constants.VIAGEM_INFORMADA_BASE_URL,
        raw_filepath=context.raw_filepath,
        params=params,
        headers=headers,
    )
