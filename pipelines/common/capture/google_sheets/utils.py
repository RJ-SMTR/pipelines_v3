# -*- coding: utf-8 -*-
"""
Utilidades para captura de abas de Google Sheets
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd

from pipelines.common.utils.gcp.bigquery import SourceTable


@dataclass
class GoogleSheetTable:
    """
    Configuração de captura de uma aba de Google Sheets.

    Attributes:
        table_id (str): table_id no BigQuery.
        sheet_name (str): Nome da aba na planilha.
        primary_keys (list[str]): Lista com o nome das primary keys da tabela.
        spread_sheet_id (Optional[str]): ID da planilha. Se None, usa o ID padrão
            passado para `create_google_sheet_capture_params`.
        filter_expr (Optional[str]): Expressão de filtro aplicada via `pd.DataFrame.query`.
        rename_mapping (Optional[dict[str, str]]): Mapeamento de renomeação de colunas. Se None,
            usa o mapeamento padrão passado para `create_google_sheet_capture_params`.
        dtypes (Optional[str | type | dict]): Mapeamento ``coluna -> dtype``.
        parse_dates (Optional[dict[str, dict]]): Mapeamento ``coluna -> kwargs`` repassados a
            ``pd.to_datetime``.
        first_timestamp (Optional[datetime]): Primeira timestamp com dados. Se None, usa a
            timestamp padrão passada para `create_google_sheet_capture_params`.
        pretreatment_reader_args (Optional[dict]): Argumentos para leitura dos dados.
        pretreat_funcs (Optional[list[Callable]]): Funções executadas antes de transformar
            em nested.
        partition_date_only (bool): Caso positivo, cria apenas a partição de data.
        max_recaptures (int): Número máximo de recapturas executadas de uma só vez.
    """

    table_id: str
    sheet_name: str
    primary_keys: list[str]
    spread_sheet_id: Optional[str] = None
    filter_expr: Optional[str] = None
    rename_mapping: Optional[dict[str, str]] = None
    dtypes: Optional[str | type | dict[str, Any]] = None
    parse_dates: Optional[dict[str, dict]] = None
    first_timestamp: Optional[datetime] = None
    pretreatment_reader_args: Optional[dict] = None
    pretreat_funcs: Optional[list[Callable[..., pd.DataFrame]]] = None
    partition_date_only: bool = True
    max_recaptures: int = 4


def create_google_sheet_capture_params(  # noqa: PLR0913
    source_name: str,
    flow_folder_name: str,
    spread_sheet_id: str,
    bucket_names: dict[str, str],
    first_timestamp: datetime,
    tables: list[GoogleSheetTable],
    rename_mapping: Optional[dict[str, str]] = None,
) -> tuple[list[SourceTable], dict[str, dict]]:
    """
    Cria os parâmetros de captura de abas de Google Sheets.

    Constrói, a partir de uma lista de `GoogleSheetTable`, o par (sources, extra_parameters)
    esperado por `create_capture_flows_default_tasks`, para uso com a task
    `create_google_sheet_extractor`.

    Args:
        source_name (str): Nome da fonte de dados.
        flow_folder_name (str): Nome da pasta do flow de captura.
        spread_sheet_id (str): ID padrão da planilha, usado quando a tabela não define o seu.
        bucket_names (dict[str, str]): Nome dos buckets de prod e dev.
        first_timestamp (datetime): Primeira timestamp padrão, usada quando a tabela não
            define a sua.
        tables (list[GoogleSheetTable]): Configurações das abas a serem capturadas.
        rename_mapping (Optional[dict[str, str]]): Mapeamento padrão de renomeação de colunas,
            usado quando a tabela não define o seu.

    Returns:
        tuple[list[SourceTable], dict[str, dict]]: Lista de SourceTable e extra_parameters
            mapeados por table_id.
    """
    sources = [
        SourceTable(
            source_name=source_name,
            table_id=table.table_id,
            first_timestamp=table.first_timestamp or first_timestamp,
            flow_folder_name=flow_folder_name,
            primary_keys=table.primary_keys,
            pretreatment_reader_args=table.pretreatment_reader_args,
            pretreat_funcs=table.pretreat_funcs,
            bucket_names=bucket_names,
            partition_date_only=table.partition_date_only,
            max_recaptures=table.max_recaptures,
            raw_filetype="csv",
        )
        for table in tables
    ]

    extra_parameters = {
        table.table_id: {
            "spread_sheet_id": table.spread_sheet_id or spread_sheet_id,
            "sheet_name": table.sheet_name,
            "filter_expr": table.filter_expr,
            "rename_mapping": table.rename_mapping or rename_mapping,
            "dtypes": table.dtypes,
            "parse_dates": table.parse_dates,
        }
        for table in tables
    }

    return sources, extra_parameters
