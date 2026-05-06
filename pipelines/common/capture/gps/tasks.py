# -*- coding: utf-8 -*-
"""
Tasks de captura dos dados de GPS (cittati, conecta, zirix, sppo, sonda)
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
    """Cria a extração de dados de GPS das fontes cittati, conecta, zirix, sppo e sonda"""
    source = context.source
    source_config = constants.GPS_SOURCE_CONFIGS[source.source_name]

    if source.source_name == constants.SONDA_SOURCE_NAME:
        secret_path = source_config["secret_path"]
        url = source_config["base_url"]
        extractor_kwargs = {"response_key": source_config.get("registros_response_key")}
    else:
        tz_name = source_config.get("timezone", "UTC")
        timestamp = context.timestamp.astimezone(timezone(tz_name))

        if source.table_id == constants.REGISTROS_TABLE_ID:
            secret_path = source_config.get(
                "registros_secret_path", source_config.get("secret_path")
            )
            endpoint = source_config["registros_endpoint"]
            fmt = source_config["registros_datetime_format"]
            start = (timestamp - timedelta(minutes=6)).strftime(fmt)
            end = (timestamp - timedelta(minutes=5)).strftime(fmt)
        else:
            secret_path = source_config.get(
                "realocacao_secret_path", source_config.get("secret_path")
            )
            endpoint = source_config["realocacao_endpoint"]
            fmt = source_config["realocacao_datetime_format"]
            start = (timestamp - timedelta(minutes=10)).strftime(fmt)
            end = timestamp.strftime(fmt)

        url = f"{source_config['base_url']}/{endpoint}"
        extractor_kwargs = {"params": {"dataInicial": start, "dataFinal": end}}

    headers = get_env_secret(secret_path)
    if not headers:
        raise ValueError(f"Empty credentials for {secret_path}")

    if source.source_name == constants.SONDA_SOURCE_NAME:
        extractor_kwargs["headers"] = headers
    else:
        key = next(iter(headers))
        extractor_kwargs["params"]["guidIdentificacao"] = headers[key]

    return partial(
        get_raw_api,
        url=url,
        raw_filepath=context.raw_filepath,
        **extractor_kwargs,
    )
