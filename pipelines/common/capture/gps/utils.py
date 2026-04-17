# -*- coding: utf-8 -*-
"""
Funções de pré-tratamento para captura de dados de GPS do SPPO.
"""

from datetime import datetime

import pandas as pd

from pipelines.common import constants as smtr_constants

OUTPUT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

SPPO_REGISTROS_RENAME = {
    "ordem": "id_veiculo",
    "linha": "servico",
    "datahora": "datetime",
    "datahoraenvio": "datetime_envio",
    "datahoraservidor": "datetime_servidor",
}

SPPO_REGISTROS_DATETIME_COLS = ["datetime", "datetime_envio", "datetime_servidor"]

SPPO_REALOCACAO_RENAME = {
    "veiculo": "id_veiculo",
    "dataOperacao": "datetime_operacao",
    "linha": "servico",
    "dataEntrada": "datetime_entrada",
    "dataSaida": "datetime_saida",
    "dataProcessado": "datetime_processamento",
}

SPPO_REALOCACAO_DATETIME_COLS = [
    "datetime_operacao",
    "datetime_entrada",
    "datetime_saida",
    "datetime_processamento",
]

REALOCACAO_DATETIME_INPUT_FORMATS = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]


def _convert_epoch_ms_to_utc_iso(series: pd.Series) -> pd.Series:
    """Converte epoch (ms) em string ISO UTC."""
    converted = pd.to_datetime(
        pd.to_numeric(series, errors="coerce"), unit="ms", utc=True, errors="coerce"
    )
    return converted.dt.strftime(OUTPUT_DATETIME_FORMAT)


def _convert_naive_sp_to_utc_iso(series: pd.Series) -> pd.Series:
    """Converte string ISO sem timezone (assumida em America/Sao_Paulo) em string ISO UTC.

    Tenta múltiplos formatos. Valores que não casam com nenhum formato (e.g. sentinelas
    como '1971-01-01 00:00:00-0300') são preservados — o staging trata esses casos.
    """
    parsed = pd.Series(pd.NaT, index=series.index)
    for fmt in REALOCACAO_DATETIME_INPUT_FORMATS:
        attempt = pd.to_datetime(series, format=fmt, errors="coerce")
        parsed = parsed.fillna(attempt)

    converted = (
        parsed.dt.tz_localize(smtr_constants.TIMEZONE, nonexistent="shift_forward", ambiguous="NaT")
        .dt.tz_convert("UTC")
        .dt.strftime(OUTPUT_DATETIME_FORMAT)
    )
    return converted.fillna(series)


def rename_sppo_registros(
    data: pd.DataFrame,
    timestamp: datetime,  # noqa: ARG001
    primary_keys: list[str],  # noqa: ARG001
) -> pd.DataFrame:
    """Renomeia colunas e converte datetimes (epoch ms → ISO UTC) dos registros do SPPO."""
    data = data.rename(columns=SPPO_REGISTROS_RENAME)
    for col in SPPO_REGISTROS_DATETIME_COLS:
        if col in data.columns:
            data[col] = _convert_epoch_ms_to_utc_iso(data[col])
    return data


def rename_sppo_realocacao(
    data: pd.DataFrame,
    timestamp: datetime,  # noqa: ARG001
    primary_keys: list[str],  # noqa: ARG001
) -> pd.DataFrame:
    """Renomeia colunas e converte datetimes (naive SP → ISO UTC) das realocações do SPPO."""
    data = data.rename(columns=SPPO_REALOCACAO_RENAME)
    for col in SPPO_REALOCACAO_DATETIME_COLS:
        if col in data.columns:
            data[col] = _convert_naive_sp_to_utc_iso(data[col])
    return data
