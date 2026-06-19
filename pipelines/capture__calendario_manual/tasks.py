# -*- coding: utf-8 -*-
"""Tasks de captura do calendário manual (datas atípicas)"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__calendario_manual import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx


@task(cache_policy=NO_CACHE)
def create_calendario_manual_extractor(context: SourceCaptureContext):
    """Cria a extração da aba 'Dias Atípicos' da planilha do calendário manual"""
    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=constants.CALENDARIO_MANUAL_SHEET_ID,
        sheet_name=constants.CALENDARIO_MANUAL_SHEET_NAME,
        raw_filepath=context.raw_filepath,
        rename_mapping=constants.CALENDARIO_MANUAL_RENAME_MAPPING,
    )
