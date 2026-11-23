# -*- coding: utf-8 -*-
"""Funções auxiliares para captura e pré-tratamento de dados de GPS."""

from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import polars as pl

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

    if len(credentials) != 1:
        raise ValueError(
            "GPS authentication must define 'secret_name' when multiple credentials are available"
        )

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


def parse_epoch_ms(column: str) -> pl.Expr:
    """Converte epoch (ms), numérico ou texto, em datetime UTC. Valores inválidos viram nulo."""
    epoch = pl.col(column).cast(pl.String).cast(pl.Float64, strict=False)
    return pl.from_epoch(epoch.cast(pl.Int64, strict=False), time_unit="ms").dt.replace_time_zone(
        "UTC"
    )


def convert_epoch_ms_to_utc_iso(column: str) -> pl.Expr:
    """Converte epoch (ms) em string ISO UTC."""
    return parse_epoch_ms(column).dt.strftime(OUTPUT_DATETIME_FORMAT).alias(column)


def convert_naive_sp_to_utc_iso(column: str) -> pl.Expr:
    """Converte string ISO sem timezone (assumida em America/Sao_Paulo) em string ISO UTC.

    Tenta múltiplos formatos. Valores que não casam com nenhum formato (e.g. sentinelas
    como '1971-01-01 00:00:00-0300') são preservados — o staging trata esses casos.
    """
    value = pl.col(column).cast(pl.String)
    parsed = pl.coalesce(
        value.str.to_datetime(format=fmt, time_unit="us", strict=False)
        for fmt in REALOCACAO_DATETIME_INPUT_FORMATS
    )
    converted = (
        parsed.dt.replace_time_zone(smtr_constants.TIMEZONE, ambiguous="null", non_existent="null")
        .dt.convert_time_zone("UTC")
        .dt.strftime(OUTPUT_DATETIME_FORMAT)
    )
    return converted.fill_null(value).alias(column)


def pretreat_sonda_registros(
    data: pl.DataFrame,
    context: SourceCaptureContext,
) -> pl.DataFrame:
    """
    Renomeia colunas, converte datetimes e filtra registros antigos (> 1 min) do Sonda (BRT).
    """
    data = data.rename(SONDA_REGISTROS_RENAME, strict=False)
    capture_utc = context.timestamp.astimezone(ZoneInfo("UTC"))

    # Converte para datetime para realizar o filtro
    gps_datetime = parse_epoch_ms("datetime")

    # Remove registros com diferença maior que 1 minuto
    diff = pl.lit(capture_utc, dtype=pl.Datetime("us", "UTC")) - gps_datetime
    data = data.filter(diff.is_between(timedelta(0), timedelta(minutes=1)))

    # Formata e preenche campos auxiliares para o staging_gps.sql
    data = data.with_columns(convert_epoch_ms_to_utc_iso("datetime"))
    return data.with_columns(
        pl.col("datetime").alias("datetime_envio"),
        pl.lit(capture_utc.strftime(OUTPUT_DATETIME_FORMAT)).alias("datetime_servidor"),
    )


def rename_sppo_registros(
    data: pl.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pl.DataFrame:
    """Renomeia colunas e converte datetimes (epoch ms → ISO UTC) dos registros do SPPO."""
    data = data.rename(SPPO_REGISTROS_RENAME, strict=False)
    return data.with_columns(
        convert_epoch_ms_to_utc_iso(c) for c in SPPO_REGISTROS_DATETIME_COLS if c in data.columns
    )


def rename_sppo_realocacao(
    data: pl.DataFrame,
    context: SourceCaptureContext,  # noqa: ARG001
) -> pl.DataFrame:
    """Renomeia colunas e converte datetimes (naive SP → ISO UTC) das realocações do SPPO."""
    data = data.rename(SPPO_REALOCACAO_RENAME, strict=False)
    return data.with_columns(
        convert_naive_sp_to_utc_iso(c) for c in SPPO_REALOCACAO_DATETIME_COLS if c in data.columns
    )
