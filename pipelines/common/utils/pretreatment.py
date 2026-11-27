# -*- coding: utf-8 -*-
"""Functions to pretreat data"""

import re
import unicodedata
from datetime import datetime
from typing import Callable, Optional, get_type_hints

import numpy as np
import pandas as pd
import polars as pl
import pytz

from pipelines.common import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


def normalize_text(
    text: str,
    snake_case: bool = False,
    case: Optional[str] = None,
    remove_multiple_spaces: bool = True,
) -> str:
    """
    Normaliza um texto removendo acentos e caracteres especiais.

    Args:
        text (str): Texto a ser normalizado.
        snake_case (bool): Se deve substituir espaços por underscores.
        case (str): "lower", "upper" ou None.
        remove_multiple_spaces (bool): Se deve remover espaços múltiplos.

    Returns:
        str: Texto normalizado.
    """
    if not text:
        return ""

    text = unicodedata.normalize("NFD", text)
    text = re.sub(r"[^A-Za-z0-9 ]", "", text)

    if remove_multiple_spaces:
        text = re.sub(r"\s+", " ", text).strip()

    if snake_case:
        text = text.replace(" ", "_")

    if case == "lower":
        text = text.lower()
    elif case == "upper":
        text = text.upper()

    return text


def transform_to_nested_structure(
    data: pd.DataFrame,
    primary_keys: list,
) -> pd.DataFrame:
    """
    Transforma colunas do DataFrame na coluna content no formato Json
    agrupando pelas primary keys

    Args:
        data (pd.DataFrame): DataFrame para aplicar o tratamento
        primary_keys (list): Lista de primary keys

    Returns:
        pd.DataFrame: Dataframe contendo as colunas listadas nas primary keys + coluna content
    """
    content_columns = [c for c in data.columns if c not in primary_keys]
    data["content"] = data[content_columns].to_json(orient="records", lines=True).splitlines()
    return data[[*primary_keys, "content"]]


def create_nested_content(data: pl.DataFrame, primary_keys: list) -> pl.DataFrame:
    """
    Agrupa as colunas que não são primary keys na coluna content, no formato JSON

    Args:
        data (pl.DataFrame): DataFrame para aplicar o tratamento
        primary_keys (list): Lista de primary keys

    Returns:
        pl.DataFrame: DataFrame contendo as colunas listadas nas primary keys + coluna content
    """
    content_columns = [c for c in data.columns if c not in primary_keys]
    return data.select(
        *primary_keys,
        pl.struct(content_columns).struct.json_encode().alias("content"),
    )


def get_pretreat_data_type(func: Callable) -> type:
    """
    Retorna o tipo de DataFrame esperado por uma função de pré-tratamento,
    a partir da anotação do argumento `data`

    Args:
        func (Callable): Função de pré-tratamento

    Returns:
        type: pl.DataFrame ou pd.DataFrame
    """
    hint = get_type_hints(func).get("data")
    return pl.DataFrame if hint is pl.DataFrame else pd.DataFrame


def run_pretreat_funcs(
    data: pl.DataFrame,
    pretreat_funcs: list[Callable],
    context: SourceCaptureContext,
) -> pl.DataFrame:
    """
    Executa as funções de pré-tratamento de um SourceTable. Funções anotadas com
    pd.DataFrame recebem os dados convertidos para pandas

    Args:
        data (pl.DataFrame): Bloco de dados brutos
        pretreat_funcs (list[Callable]): Funções de pré-tratamento
        context (SourceCaptureContext): Contexto da captura

    Returns:
        pl.DataFrame: Dados pré-tratados
    """
    for func in pretreat_funcs:
        if get_pretreat_data_type(func) is pl.DataFrame:
            data = func(data=data, context=context)
        else:
            # Nulos como NaN, igual à leitura do pandas, para manter o comportamento das funções
            pandas_data = data.to_pandas().fillna(np.nan)
            data = pl.from_pandas(func(data=pandas_data, context=context))
    return data


def strip_string_columns(
    data: pl.DataFrame,
    context: Optional[SourceCaptureContext] = None,  # noqa: ARG001
) -> pl.DataFrame:
    """
    Remove espaços no início e no fim de todas as colunas de texto
    de um DataFrame. Pode ser usada como pretreat_func de um SourceTable.

    Args:
        data (pl.DataFrame): DataFrame a ser tratado
        context (Optional[SourceCaptureContext]): Contexto da captura (não utilizado)

    Returns:
        pl.DataFrame: DataFrame tratado
    """
    return data.with_columns(pl.col(pl.String).str.strip_chars())


def raise_if_column_isna(
    column_name: str,
) -> Callable[[pd.DataFrame, datetime, list], pd.DataFrame]:
    """
    Cria uma função de validação que lança um erro caso a coluna especificada contenha valores nulos

    Args:
        column_name (str): Nome da coluna a ser validada

    Returns:
        Callable[[pd.DataFrame, datetime, list], pd.DataFrame]:
        Uma função que recebe um DataFrame, um timestamp e chaves primárias,
        validando a coluna especificada
    """

    def func(
        data: pd.DataFrame,
        context: SourceCaptureContext,  # noqa: ARG001
    ):
        if not data[data[column_name].isna()].empty:
            raise ValueError(f"A coluna {column_name} está nula")
        return data

    return func


def create_timestamp_captura(timestamp: datetime) -> str:
    """
    Cria o valor para a coluna timestamp_captura

    Args:
        timestamp (datetime): timestamp a ser escrita

    Returns:
        str: Valor a ser escrito na coluna timestamp_captura
    """

    return timestamp.astimezone(tz=pytz.timezone(constants.TIMEZONE)).strftime(
        "%Y-%m-%d %H:%M:%S-03:00"
    )
