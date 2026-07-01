# -*- coding: utf-8 -*-
"""Tasks de captura do calendário manual (datas atípicas)"""

from datetime import datetime
from functools import partial
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__calendario_manual import constants
from pipelines.capture__calendario_manual.utils import (
    get_calendario_last_row_state,
    get_calendario_redis_key,
    get_calendario_sheet_df,
    get_changed_dates_by_last_row_state,
)
from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture.utils import ShouldCapture, SourceCaptureContext
from pipelines.common.utils.extractors.gdrive import get_google_sheet_xlsx
from pipelines.common.utils.redis import get_redis_client


@task(cache_policy=NO_CACHE)
def create_calendario_manual_extractor(context: SourceCaptureContext):
    """
    Cria a função de extração da planilha do calendário manual.

    Args:
        context: Contexto da captura com o caminho do arquivo bruto.

    Returns:
        Função configurada para extrair a aba do calendário manual.
    """
    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=constants.CALENDARIO_MANUAL_SHEET_ID,
        sheet_name=constants.CALENDARIO_MANUAL_SHEET_NAME,
        dtypes=str,
        parse_dates=constants.CALENDARIO_MANUAL_PARSE_DATES,
        filter_expr=constants.CALENDARIO_MANUAL_FILTER_EXPR,
        raw_filepath=context.raw_filepath,
    )


@task(cache_policy=NO_CACHE)
def detect_calendario_change(env: str) -> ShouldCapture:
    """
    Verifica se o calendário manual mudou desde a última captura.

    Args:
        env: Ambiente usado para selecionar a chave Redis.

    Returns:
        Decisão de captura e, no payload, o estado já calculado da planilha.
    """
    df = get_calendario_sheet_df()
    current_state = get_calendario_last_row_state(df)

    previous_state = get_redis_client().get(get_calendario_redis_key(env))
    changed_dates = get_changed_dates_by_last_row_state(
        df=df,
        previous_state=previous_state,
        current_state=current_state,
    )
    changed = bool(changed_dates)

    print(f"Datas alteradas: {changed_dates} | mudou: {changed}")
    return ShouldCapture(
        value=changed,
        payload={
            "last_row_state": current_state,
            "changed_dates": changed_dates,
        },
    )


@task(cache_policy=NO_CACHE)
def update_calendario_last_row_state(env: str, last_row_state: dict[str, int | str]) -> None:
    """
    Persiste no Redis o estado da última linha submetida da planilha.

    Args:
        env: Ambiente usado para selecionar a chave Redis.
        last_row_state: Estado atual com ``last_index`` e ``last_hash``.
    """
    client = get_redis_client()
    key = get_calendario_redis_key(env)
    client.set(key, last_row_state)
    print(f"Estado da última linha atualizado no Redis: {last_row_state}")


@task(cache_policy=NO_CACHE)
def get_calendario_materialization_window(
    changed_dates: list[str],
) -> Optional[tuple[str, str]]:
    """
    Calcula a janela de materialização a partir do estado apurado em `detect_calendario_change`.

    Args:
        changed_dates: Datas novas ou alteradas calculadas em `detect_calendario_change`.

    Returns:
        Data inicial e data final da janela de materialização. Retorna ``None`` quando só há datas
        alteradas anteriores à data atual.
    """
    today = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).strftime("%Y-%m-%d")
    future_changed_dates = [date for date in changed_dates if date >= today]
    if not future_changed_dates:
        print("Materialização não disparada: sem datas futuras alteradas")
        return None

    datetime_start = min(future_changed_dates)
    datetime_end = max(future_changed_dates)

    print(f"Janela de materialização: {datetime_start} → {datetime_end}")
    return datetime_start, datetime_end
