# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de fiscalização de veículos"""

from pathlib import Path

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__veiculo_fiscalizacao_lacre import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx
from pipelines.common.utils.fs import save_local_file


@task(cache_policy=NO_CACHE)
def create_veiculo_fiscalizacao_lacre_extractor(context: SourceCaptureContext):
    """Cria a extração da planilha de controle de lacre dos veículos"""
    df = get_google_sheet_xlsx(
        spread_sheet_id=constants.VEICULO_LACRE_SHEET_ID,
        sheet_name=constants.VEICULO_LACRE_SHEET_NAME,
    )

    filepath = Path(context.raw_filepath).parent / "veiculo_fiscalizacao_lacre.csv"
    save_local_file(filepath=str(filepath), filetype="csv", data=df)

    return filepath
