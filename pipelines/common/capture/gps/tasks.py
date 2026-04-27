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
    source_config = constants.GPS_SOURCE_CONFIGS[source.source_name]
    tz_name = source_config.get("timezone", "UTC")
    timestamp = context.timestamp.astimezone(timezone(tz_name))

    if source.table_id == constants.REGISTROS_TABLE_ID:
        endpoint = source_config["registros_endpoint"]
        secret_path = source_config.get("registros_secret_path", source_config.get("secret_path"))
        fmt = source_config.get("registros_datetime_format", "%Y-%m-%d %H:%M:%S")
        date_range_start = (timestamp - timedelta(minutes=6)).strftime(fmt)
        date_range_end = (timestamp - timedelta(minutes=5)).strftime(fmt)
    else:
        endpoint = source_config["realocacao_endpoint"]
        secret_path = source_config.get("realocacao_secret_path", source_config.get("secret_path"))
        fmt = source_config.get("realocacao_datetime_format", "%Y-%m-%d %H:%M:%S")
        date_range_start = (timestamp - timedelta(minutes=10)).strftime(fmt)
        date_range_end = timestamp.strftime(fmt)

    url = f"{source_config['base_url']}/{endpoint}"

    headers = get_env_secret(secret_path)
    if not headers:
        raise ValueError(f"Empty credentials for {secret_path}")
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
