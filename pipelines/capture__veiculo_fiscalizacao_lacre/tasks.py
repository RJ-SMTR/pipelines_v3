# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de fiscalização de veículos"""

from functools import partial

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__veiculo_fiscalizacao_lacre import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx
from pipelines.common.utils.pretreatment import normalize_text

VEICULO_LACRE_RENAME = {
    "PLACA": "placa",
    "N DE ORDEM": "n_o_de_ordem",
    "DATA DO LACRE": "data_do_lacre",
    "N DO AUTO": "n_do_auto",
    "N DO LACRE": "n_do_lacre",
    "DATA DO DESLACRE": "data_do_deslacre",
    "NOME DO FISCAL DESLACRE": "nome_do_fiscal_deslacre",
    "DIAS LACRADOS": "dias_lacrados",
    "MOTIVO DO LACRE": "motivo_do_lacre",
    "ULTIMO EDITOR": "ultimo_editor",
    "ULTIMA ATUALIZACAO": "ultima_atualizacao",
}


def rename_veiculo_lacre_columns(
    data: pd.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pd.DataFrame:
    """Normaliza os nomes das colunas da planilha de controle de lacre."""
    normalized_columns = {}
    for col in data.columns:
        if col in VEICULO_LACRE_RENAME:
            normalized_columns[col] = VEICULO_LACRE_RENAME[col]
        else:
            normalized_columns[col] = normalize_text(col, snake_case=True, case="lower")
    data = data.rename(columns=normalized_columns)
    return data


@task(cache_policy=NO_CACHE)
def create_veiculo_fiscalizacao_lacre_extractor(context: SourceCaptureContext):
    """Cria a extração da planilha de controle de lacre dos veículos"""
    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=constants.VEICULO_LACRE_SHEET_ID,
        sheet_name=constants.VEICULO_LACRE_SHEET_NAME,
        raw_filepath=context.raw_filepath,
    )
