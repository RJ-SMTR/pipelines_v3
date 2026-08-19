# -*- coding: utf-8 -*-
"""Funções auxiliares para captura e pré-tratamento de dados de GPS."""

from datetime import datetime, timedelta
from typing import Optional
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
from pipelines.common.utils.secret import get_env_secret


def get_gps_request_config(
    source_config: dict,
    table_id: str,
) -> dict:
    """Retorna a configuração de request de uma tabela de GPS."""
    try:
        return source_config["requests"][table_id]
    except KeyError as exc:
        raise ValueError(f"GPS table '{table_id}' is not configured") from exc


def create_gps_capture_params(
    window_config: dict,
    timestamp: datetime,
) -> dict[str, str]:
    """Cria os parâmetros da janela temporal de uma captura de GPS."""
    timestamp = timestamp.astimezone(ZoneInfo(window_config.get("timezone", "UTC")))
    start = timestamp - timedelta(minutes=window_config["start_offset_minutes"])
    end = timestamp - timedelta(minutes=window_config["end_offset_minutes"])
    return {
        window_config.get("start_parameter", "dataInicial"): start.strftime(
            window_config["datetime_format"]
        ),
        window_config.get("end_parameter", "dataFinal"): end.strftime(
            window_config["datetime_format"]
        ),
    }


def create_gps_authentication_kwargs(
    auth_config: dict,
    params: Optional[dict[str, str]] = None,
) -> dict:
    """Cria headers ou query parameters para autenticação de uma API de GPS."""
    credentials = get_env_secret(
        secret_path=auth_config["secret_path"],
        secret_name=auth_config.get("secret_name"),
    )
    request_kwargs = {"params": dict(params)} if params is not None else {}

    if auth_config.get("credentials_as_headers"):
        request_kwargs["headers"] = credentials
        return request_kwargs

    credential_value = next(iter(credentials.values()))
    header = auth_config.get("header")
    if header is not None:
        scheme = auth_config.get("scheme")
        if scheme is not None:
            credential_value = f"{scheme} {credential_value}"
        request_kwargs["headers"] = {header: credential_value}
        return request_kwargs

    parameter = auth_config.get("parameter", "guidIdentificacao")
    request_kwargs.setdefault("params", {})[parameter] = credential_value
    return request_kwargs


def create_gps_extractor_kwargs(
    context: SourceCaptureContext,
    source_config: dict,
) -> dict:
    """Cria os argumentos do extrator de uma captura de GPS."""
    request_config = get_gps_request_config(
        source_config=source_config,
        table_id=context.source.table_id,
    )
    url = source_config["base_url"]
    endpoint = request_config.get("endpoint")
    if endpoint is not None:
        url = f"{url.rstrip('/')}/{endpoint.lstrip('/')}"

    params = None
    window_config = request_config.get("window")
    if window_config is not None:
        params = create_gps_capture_params(
            window_config=window_config,
            timestamp=context.timestamp,
        )

    auth_config = request_config.get("auth") or source_config.get("auth")
    if auth_config is None:
        raise ValueError(f"GPS table '{context.source.table_id}' has no auth config")

    extractor_kwargs = {
        "url": url,
        "response_key": request_config.get("response_key"),
    }
    extractor_kwargs.update(
        create_gps_authentication_kwargs(auth_config=auth_config, params=params)
    )
    return extractor_kwargs


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
