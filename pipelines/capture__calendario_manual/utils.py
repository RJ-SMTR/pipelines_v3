# -*- coding: utf-8 -*-
"""Utilitários da captura do calendário manual (datas atípicas)."""

import hashlib

import pandas as pd

from pipelines.capture__calendario_manual import constants
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx


def get_calendario_row_hash(row: pd.Series) -> str:
    """
    Calcula um hash SHA-256 para uma linha do calendário manual.

    Args:
        row: Linha validada da planilha.

    Returns:
        Hash da linha considerando as colunas de conteúdo do calendário manual.
    """
    row_df = pd.DataFrame([row[constants.CALENDARIO_MANUAL_COLUMNS]])
    row_df["dia"] = row_df["dia"].dt.strftime("%Y-%m-%d")
    row_df = row_df.fillna("").astype(str)
    return hashlib.sha256(row_df.to_csv(index=False, header=False).encode("utf-8")).hexdigest()


def get_calendario_last_row_state(df: pd.DataFrame) -> dict[str, int | str] | None:
    """
    Calcula o estado compacto da última linha submetida da planilha.

    Args:
        df: Dados validados da planilha, mantendo o índice original das linhas.

    Returns:
        Dicionário com o índice original da última linha e o hash do seu conteúdo, ou ``None``
        quando não há linhas submetidas.
    """
    if df.empty:
        return None

    last_index = int(df.index.max())
    return {
        "last_index": last_index,
        "last_hash": get_calendario_row_hash(df.loc[last_index]),
    }


def get_changed_dates_by_last_row_state(
    df: pd.DataFrame,
    previous_state: dict[str, int | str] | None,
    current_state: dict[str, int | str] | None,
) -> list[str]:
    """
    Identifica datas a materializar comparando apenas o estado da última linha submetida.

    A planilha é tratada como append-only: novas linhas no fim avançam o índice, e uma edição na
    última linha muda seu hash. Alterações em linhas anteriores não disparam captura.

    Args:
        df: Dados validados da planilha, mantendo o índice original das linhas.
        previous_state: Estado persistido no Redis após a captura anterior.
        current_state: Estado atual calculado da última linha submetida.

    Returns:
        Datas das novas linhas, ou da última linha quando apenas o hash dela mudou.
    """
    if df.empty:
        return []

    if current_state is None:
        raise ValueError("Estado atual da última linha não foi calculado para planilha não vazia")

    if not previous_state:
        changed_df = df
    else:
        previous_last_index = int(previous_state["last_index"])
        current_last_index = int(current_state["last_index"])

        if current_last_index == previous_last_index:
            if current_state["last_hash"] == previous_state["last_hash"]:
                return []
            changed_df = df.loc[[current_last_index]]
        elif current_last_index > previous_last_index:
            changed_df = df.loc[(df.index > previous_last_index) & (df.index <= current_last_index)]
        else:
            raise ValueError(
                "Índice da última linha submetida diminuiu: "
                f"anterior={previous_last_index}, atual={current_last_index}"
            )

    return sorted(changed_df["dia"].dt.strftime("%Y-%m-%d").unique())


def get_calendario_redis_key(env: str) -> str:
    """
    Monta a chave Redis usada para persistir os hashes por data.

    Args:
        env: Ambiente de execução.

    Returns:
        Chave com prefixo do ambiente quando ele não é ``prod``.
    """
    key = (
        f"source_{constants.CALENDARIO_MANUAL_SOURCE_NAME}."
        f"{constants.CALENDARIO_MANUAL_TABLE_ID}.last_row_state"
    )
    if env != "prod":
        key = f"{env}.{key}"
    return key


def get_calendario_sheet_df() -> pd.DataFrame:
    """
    Lê e valida os dados relevantes da planilha do calendário manual.

    Linhas sem data válida, anteriores à data de corte ou não submetidas são descartadas.

    Returns:
        DataFrame com uma linha por data, mantendo o índice original das linhas na planilha.

    Raises:
        ValueError: Se faltarem colunas obrigatórias ou houver datas duplicadas.
    """
    df = get_google_sheet_xlsx(
        spread_sheet_id=constants.CALENDARIO_MANUAL_SHEET_ID,
        sheet_name=constants.CALENDARIO_MANUAL_SHEET_NAME,
        dtypes=str,
        parse_dates=constants.CALENDARIO_MANUAL_PARSE_DATES,
        filter_expr=constants.CALENDARIO_MANUAL_FILTER_EXPR,
    )

    required_columns = {
        *constants.CALENDARIO_MANUAL_COLUMNS,
        constants.CALENDARIO_MANUAL_SUBMIT_COLUMN,
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas ausentes na planilha: {sorted(missing_columns)}")

    duplicated_dates = df.loc[df["dia"].duplicated(keep=False), "dia"]
    if not duplicated_dates.empty:
        dates = sorted(duplicated_dates.dt.strftime("%Y-%m-%d").unique())
        raise ValueError(f"Datas duplicadas na planilha: {dates}")

    return df
