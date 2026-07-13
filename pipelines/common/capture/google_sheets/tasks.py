# -*- coding: utf-8 -*-
"""
Tasks de captura de abas de Google Sheets
"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx


@task(cache_policy=NO_CACHE)
def create_google_sheet_extractor(context: SourceCaptureContext):
    """Cria a extração de uma aba de Google Sheets"""
    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=context.extra_parameters["spread_sheet_id"],
        sheet_name=context.extra_parameters["sheet_name"],
        filter_expr=context.extra_parameters.get("filter_expr"),
        raw_filepath=context.raw_filepath,
        rename_mapping=context.extra_parameters.get("rename_mapping"),
        dtypes=context.extra_parameters.get("dtypes"),
        parse_dates=context.extra_parameters.get("parse_dates"),
    )
