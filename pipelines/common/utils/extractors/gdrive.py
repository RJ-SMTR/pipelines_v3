# -*- coding: utf-8 -*-
"""Module to get data from Google Drive"""

import os
from typing import Any, Optional

import pandas as pd
from google.auth import default
from google.oauth2 import service_account
from googleapiclient.discovery import build

from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.pretreatment import normalize_text


def get_google_api_service(service_name: str, version: str, scopes: Optional[list[str]] = None):
    """
    Retorna um serviço do Google API configurado com as credenciais apropriadas.

    Args:
        service_name: Nome do serviço Google (ex: 'sheets', 'drive')
        version: Versão da API (ex: 'v4', 'v3')
        scopes: Lista de escopos de permissão. Se None, usa drive.readonly por padrão.

    Returns:
        Resource: Serviço do Google API configurado
    """

    scopes_by_service = {
        "sheets": ["https://www.googleapis.com/auth/spreadsheets.readonly"],
        "drive": ["https://www.googleapis.com/auth/drive.readonly"],
    }

    if scopes is None:
        scopes = scopes_by_service.get(
            service_name, ["https://www.googleapis.com/auth/drive.readonly"]
        )

    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        creds, _ = default(scopes=scopes)
    else:
        creds = service_account.Credentials.from_service_account_file(
            filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
        )

    return build(service_name, version, credentials=creds)


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
            ``{"dia": {"format": "%d/%m/%Y"}}``). ``errors="coerce"`` é o padrão para não quebrar
            com valores inválidos

    Returns:
        DataFrame se raw_filepath for None, lista com filepath se raw_filepath for fornecido
    """

    sheets_service = get_google_api_service(service_name="sheets", version="v4")

    file = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spread_sheet_id,
            range=sheet_name,
        )
        .execute()
    )["values"]

    columns = file[0]
    rows = []
    for row_number, row in enumerate(file[1:], start=2):
        if len(row) > len(columns):
            raise ValueError(
                f"A linha {row_number} da aba {sheet_name} tem {len(row)} valores, "
                f"mas o cabeçalho tem {len(columns)} colunas"
            )

        rows.append([*row, *([None] * (len(columns) - len(row)))])

    df = pd.DataFrame(rows, columns=columns)

    df.columns = [
        normalize_text(c, snake_case=True, case="lower", remove_multiple_spaces=True)
        for c in df.columns
    ]

    if rename_mapping:
        df = df.rename(columns=rename_mapping)

    if dtypes:
        df = df.astype(dtypes)

    if parse_dates:
        for column, kwargs in parse_dates.items():
            df[column] = pd.to_datetime(df[column], **kwargs)

    if filter_expr:
        df = df.query(filter_expr)

    if raw_filepath:
        filepath = raw_filepath.format(page=0)
        save_local_file(filepath=filepath, filetype="csv", data=df)
        return [filepath]

    return df
