# -*- coding: utf-8 -*-
"""
Tasks de captura dos dados de GPS (cittati, conecta, zirix)
"""

from datetime import timedelta
from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE
from pytz import timezone

from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.gps import constants
from pipelines.common.utils.extractors.api import get_raw_api
from pipelines.common.utils.secret import get_env_secret


@task(cache_policy=NO_CACHE)
def create_gps_extractor(context: SourceCaptureContext):
    """Cria a extração de dados de GPS das fontes cittati, conecta e zirix"""
    source = context.source
    timestamp = context.timestamp.astimezone(timezone("UTC"))

    source_config = constants.GPS_SOURCE_CONFIGS[source.source_name]
    date_format = source_config["date_format"]

    if source.table_id == constants.REGISTROS_TABLE_ID:
        endpoint = source_config["registros_endpoint"]
        date_range_start = (timestamp - timedelta(minutes=6)).strftime(date_format)
        date_range_end = (timestamp - timedelta(minutes=5)).strftime(date_format)
    else:
        endpoint = source_config["realocacao_endpoint"]
        date_range_start = (timestamp - timedelta(minutes=10)).strftime(date_format)
        date_range_end = timestamp.strftime(date_format)

    url = f"{source_config['base_url']}/{endpoint}"

    headers = get_env_secret(source_config["secret_path"])
    if not headers:
        raise ValueError(f"Empty credentials for {source_config['secret_path']}")
    key = next(iter(headers))
    params = {
        "guidIdentificacao": headers[key],
        "dataInicial": date_range_start,
        "dataFinal": date_range_end,
    }

    return partial(
        get_raw_api,
        url=url,
        raw_filepath=context.raw_filepath,
        params=params,
    )
