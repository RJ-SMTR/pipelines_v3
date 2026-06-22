# -*- coding: utf-8 -*-
"""Tasks de captura do calendário manual (datas atípicas)"""

import hashlib
from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__calendario_manual import constants
from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture.utils import ShouldCapture, SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx
from pipelines.common.utils.redis import get_redis_client

RELEVANT_COLUMNS = ["dia", "tipo_dia", "subtipo_dia", "tipo_os", "despacho_observacao"]


def get_calendario_redis_key(env: str) -> str:
    """Monta a chave do Redis que guarda o hash da última planilha processada."""
    key = (
        f"source_{constants.CALENDARIO_MANUAL_SOURCE_NAME}."
        f"{constants.CALENDARIO_MANUAL_TABLE_ID}.last_hash"
    )
    if env != "prod":
        key = f"{env}.{key}"
    return key


def get_calendario_sheet_df() -> pd.DataFrame:
    """Lê a aba 'Dias Atípicos' e devolve as linhas com data válida >= data de corte."""
    df = get_google_sheet_xlsx(
        spread_sheet_id=constants.CALENDARIO_MANUAL_SHEET_ID,
        sheet_name=constants.CALENDARIO_MANUAL_SHEET_NAME,
    )

    missing_columns = set(RELEVANT_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas ausentes na planilha: {sorted(missing_columns)}")

    df["dia"] = pd.to_datetime(df["dia"], format="%d/%m/%Y", errors="coerce")
    cutover = pd.Timestamp(constants.CALENDARIO_MANUAL_CUTOVER_DATE)
    df = df[df["dia"].notna() & (df["dia"] >= cutover)]
    df = df.sort_values("dia").reset_index(drop=True)

    duplicated_dates = df.loc[df["dia"].duplicated(keep=False), "dia"]
    if not duplicated_dates.empty:
        dates = sorted(duplicated_dates.dt.strftime("%Y-%m-%d").unique())
        raise ValueError(f"Datas duplicadas na planilha: {dates}")

    return df


@task(cache_policy=NO_CACHE)
def create_calendario_manual_extractor(context: SourceCaptureContext):
    """Cria a extração da aba 'Dias Atípicos' da planilha do calendário manual"""
    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=constants.CALENDARIO_MANUAL_SHEET_ID,
        sheet_name=constants.CALENDARIO_MANUAL_SHEET_NAME,
        raw_filepath=context.raw_filepath,
    )


@task(cache_policy=NO_CACHE)
def detect_calendario_change(env: str) -> ShouldCapture:
    """
    Gate de mudança: compara o hash do conteúdo relevante da planilha (apenas datas >= corte)
    com o último hash salvo no Redis. Retorna `ShouldCapture` com o hash atual em `metadata`.
    """
    df = get_calendario_sheet_df()
    cols = [c for c in RELEVANT_COLUMNS if c in df.columns]
    content = df[cols].astype(str).to_csv(index=False)
    new_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

    stored = get_redis_client().get(get_calendario_redis_key(env))
    last_hash = stored.get("hash") if stored else None

    changed = new_hash != last_hash
    print(f"Hash atual: {new_hash} | último: {last_hash} | mudou: {changed}")
    return ShouldCapture(value=changed, metadata={"hash": new_hash})


@task(cache_policy=NO_CACHE)
def update_calendario_hash(env: str, new_hash: str) -> None:
    """Persiste o hash da planilha no Redis após a captura/disparo da materialização."""
    client = get_redis_client()
    key = get_calendario_redis_key(env)
    value = {"hash": new_hash}
    client.set(key, value)
    print(f"Hash atualizado no Redis: {new_hash}")


@task(cache_policy=NO_CACHE)
def get_calendario_materialization_window() -> tuple[str, str, dict]:
    """
    Janela de materialização do planejamento diário: da data de corte até o maior valor entre
    hoje e a maior data presente na planilha (cobre qualquer data editada, sem diff por linha).
    """
    df = get_calendario_sheet_df()
    today = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).strftime("%Y-%m-%d")
    max_date = df["dia"].max().strftime("%Y-%m-%d") if not df.empty else today

    datetime_start = constants.CALENDARIO_MANUAL_CUTOVER_DATE
    datetime_end = max(today, max_date, datetime_start)
    print(f"Janela de materialização: {datetime_start} → {datetime_end}")
    return datetime_start, datetime_end, {}
