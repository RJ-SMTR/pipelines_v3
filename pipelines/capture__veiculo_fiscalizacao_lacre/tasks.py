# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de fiscalização de veículos"""

from functools import partial

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__veiculo_fiscalizacao_lacre import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx
from pipelines.common.utils.fs import save_local_file


def _extract_and_save_veiculo_fiscalizacao_lacre(context: SourceCaptureContext) -> str:
    """Extrai a planilha de controle de lacre e salva como CSV"""
    df = get_google_sheet_xlsx(
        spread_sheet_id=constants.VEICULO_LACRE_SHEET_ID,
        sheet_name=constants.VEICULO_LACRE_SHEET_NAME,
    )

    save_local_file(filepath=context.raw_filepath, filetype="csv", data=df)

    return context.raw_filepath


@task(cache_policy=NO_CACHE)
def create_veiculo_fiscalizacao_lacre_extractor(context: SourceCaptureContext):  # noqa: ARG001
    """Cria a extração da planilha de controle de lacre dos veículos"""
    return partial(_extract_and_save_veiculo_fiscalizacao_lacre)
