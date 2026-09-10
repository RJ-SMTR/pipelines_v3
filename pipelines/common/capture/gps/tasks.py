# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de GPS."""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.gps.constants import GPS_SOURCE_CONFIGS
from pipelines.common.capture.gps.utils import create_gps_extractor_kwargs
from pipelines.common.utils.extractors.api import get_raw_api


@task(cache_policy=NO_CACHE)
def create_gps_extractor(context: SourceCaptureContext):
    """Cria a extração de dados de GPS das fontes configuradas."""
    source_config = GPS_SOURCE_CONFIGS[context.source.source_name]
    extractor_kwargs = create_gps_extractor_kwargs(
        context=context,
        source_config=source_config,
    )

    return partial(
        get_raw_api,
        raw_filepath=context.raw_filepath,
        **extractor_kwargs,
    )
