# -*- coding: utf-8 -*-
"""Module to get data from Google Drive"""

import io
from typing import Any, Optional

import pandas as pd
from googleapiclient.http import MediaIoBaseDownload

from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.google_drive import read_google_sheets


def download_drive_file(file_link: str, drive_service) -> bytes:
    """
    Baixa um arquivo do Google Drive. Planilhas Google são exportadas como XLSX.

    Args:
        file_link: Link de compartilhamento do arquivo (``.../d/<file_id>/...``)
        drive_service: Serviço do Google Drive retornado por ``get_google_api_service``

    Returns:
        bytes: Conteúdo do arquivo
    """
    file_id = file_link.split("/")[-2]
    file = drive_service.files().get(fileId=file_id, supportsAllDrives=True).execute()

    if "google-apps" in file.get("mimeType"):
        request = drive_service.files().export(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)

    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return file_bytes.getvalue()


def get_google_sheet_xlsx(  # noqa: PLR0913
    spread_sheet_id: str,
    sheet_name: str,
    filter_expr: Optional[str] = None,
    raw_filepath: Optional[str] = None,
    rename_mapping: Optional[dict[str, str]] = None,
    dtypes: Optional[str | type | dict[str, Any]] = None,
    parse_dates: Optional[dict[str, dict]] = None,
) -> pd.DataFrame | list[str]:
    """Extrai dados de uma planilha Google Sheets

    Args:
        spread_sheet_id: ID da planilha
        sheet_name: Nome da aba
        filter_expr: Expressão de filtro opcional
        raw_filepath: Se fornecido, salva o CSV neste caminho e retorna [filepath]
        rename_mapping: Dicionário com mapeamento de renomeação de colunas
        dtypes: Mapeamento ``coluna -> dtype``
        parse_dates: Mapeamento ``coluna -> kwargs`` repassados a ``pd.to_datetime`` (ex.:
            ``{"dia": {"format": "%d/%m/%Y"}}``)

    Returns:
        DataFrame se raw_filepath for None, lista com filepath se raw_filepath for fornecido
    """

    df = read_google_sheets(
        spread_sheet_id=spread_sheet_id,
        sheet_name=sheet_name,
        filter_expr=filter_expr,
        rename_mapping=rename_mapping,
        dtypes=dtypes,
        parse_dates=parse_dates,
    )

    if raw_filepath:
        filepath = raw_filepath.format(page=0)
        save_local_file(filepath=filepath, filetype="csv", data=df)
        return [filepath]

    return df
