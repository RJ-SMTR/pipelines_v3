# -*- coding: utf-8 -*-
"""Utilitários da captura do calendário manual (datas atípicas)."""

import hashlib

import pandas as pd

from pipelines.capture__calendario_manual import constants
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx


def get_calendario_hashes_by_date(df: pd.DataFrame) -> dict[str, str]:
    """
    Calcula um hash SHA-256 para cada data do calendário manual.

    Args:
        df: Dados validados da planilha, com uma linha por data.

    Returns:
        Dicionário no formato ``data ISO -> hash da linha``.
    """
    content_df = df[constants.CALENDARIO_MANUAL_COLUMNS].copy()
    content_df["dia"] = content_df["dia"].dt.strftime("%Y-%m-%d")
    content_df = content_df.fillna("").astype(str)

    return {
        row["dia"]: hashlib.sha256(
            pd.DataFrame([row], columns=constants.CALENDARIO_MANUAL_COLUMNS)
            .to_csv(index=False, header=False)
            .encode("utf-8")
        ).hexdigest()
        for row in content_df.to_dict(orient="records")
    }


def get_changed_dates(previous_hashes: dict[str, str], current_hashes: dict[str, str]) -> list[str]:
    """
    Identifica datas novas ou modificadas no conjunto atual.

    Args:
        previous_hashes: Hashes por data persistidos após a captura anterior.
        current_hashes: Hashes por data calculados da planilha atual.

    Returns:
        Datas novas ou modificadas em ordem crescente, no formato ISO.
    """
    return sorted(
        date for date in current_hashes if current_hashes[date] != previous_hashes.get(date)
    )


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
        f"{constants.CALENDARIO_MANUAL_TABLE_ID}.last_hashes_by_date"
    )
    if env != "prod":
        key = f"{env}.{key}"
    return key


def get_calendario_sheet_df() -> pd.DataFrame:
    """
    Lê e valida os dados relevantes da planilha do calendário manual.

    Linhas sem data válida, anteriores à data de corte ou não submetidas são descartadas.

    Returns:
        DataFrame ordenado por ``dia``, com uma linha por data.

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

    df = df.sort_values("dia").reset_index(drop=True)

    duplicated_dates = df.loc[df["dia"].duplicated(keep=False), "dia"]
    if not duplicated_dates.empty:
        dates = sorted(duplicated_dates.dt.strftime("%Y-%m-%d").unique())
        raise ValueError(f"Datas duplicadas na planilha: {dates}")

    return df
