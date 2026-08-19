# -*- coding: utf-8 -*-
"""
Funções de pré-tratamento para captura de dados de GPS.
"""

from zoneinfo import ZoneInfo

import pandas as pd

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.capture.gps.constants import (
    OUTPUT_DATETIME_FORMAT,
    REALOCACAO_DATETIME_INPUT_FORMATS,
    SONDA_REGISTROS_RENAME,
    SPPO_REALOCACAO_DATETIME_COLS,
    SPPO_REALOCACAO_RENAME,
    SPPO_REGISTROS_DATETIME_COLS,
    SPPO_REGISTROS_RENAME,
)


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


def pretreat_sonda_registros(
    data: pd.DataFrame,
    context: SourceCaptureContext,
) -> pd.DataFrame:
    """
    Renomeia colunas, converte datetimes e filtra registros antigos (> 1 min) do Sonda (BRT).
    """
    data = data.rename(columns=SONDA_REGISTROS_RENAME)

    # Converte para datetime para realizar o filtro
    gps_datetime = pd.to_datetime(
        pd.to_numeric(data["datetime"], errors="coerce"), unit="ms", utc=True
    )
    capture_utc = context.timestamp.astimezone(ZoneInfo("UTC"))

    # Remove registros com diferença maior que 1 minuto
    diff_minutes = (capture_utc - gps_datetime).dt.total_seconds() / 60
    data = data[diff_minutes.between(0, 1)].copy()

    # Formata e preenche campos auxiliares para o staging_gps.sql
    data["datetime"] = gps_datetime.dt.strftime(OUTPUT_DATETIME_FORMAT)
    data["datetime_envio"] = data["datetime"]
    data["datetime_servidor"] = capture_utc.strftime(OUTPUT_DATETIME_FORMAT)

    return data


def rename_sppo_registros(
    data: pd.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pd.DataFrame:
    """Renomeia colunas e converte datetimes (epoch ms → ISO UTC) dos registros do SPPO."""
    data = data.rename(columns=SPPO_REGISTROS_RENAME)
    for col in SPPO_REGISTROS_DATETIME_COLS:
        if col in data.columns:
            data[col] = _convert_epoch_ms_to_utc_iso(data[col])
    return data


def rename_sppo_realocacao(
    data: pd.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pd.DataFrame:
    """Renomeia colunas e converte datetimes (naive SP → ISO UTC) das realocações do SPPO."""
    data = data.rename(columns=SPPO_REALOCACAO_RENAME)
    for col in SPPO_REALOCACAO_DATETIME_COLS:
        if col in data.columns:
            data[col] = _convert_naive_sp_to_utc_iso(data[col])
    return data
