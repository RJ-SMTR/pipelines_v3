# -*- coding: utf-8 -*-
"""Tasks de captura do calendário manual (datas atípicas)"""

from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__calendario_manual import constants
from pipelines.capture__calendario_manual.utils import (
    get_calendario_hashes_by_date,
    get_calendario_redis_key,
    get_calendario_sheet_df,
    get_changed_dates,
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
        raw_filepath=context.raw_filepath,
    )


@task(cache_policy=NO_CACHE)
def detect_calendario_change(env: str) -> ShouldCapture:
    """
    Verifica se o calendário manual mudou desde a última captura.

    Args:
        env: Ambiente usado para selecionar a chave Redis.

    Returns:
        Decisão de captura e hashes atuais por data no metadata.
    """
    hashes_by_date = get_calendario_hashes_by_date(get_calendario_sheet_df())

    previous_hashes_by_date = get_redis_client().get(get_calendario_redis_key(env))
    changed = hashes_by_date != previous_hashes_by_date
    changed_dates = (
        get_changed_dates(previous_hashes_by_date, hashes_by_date)
        if previous_hashes_by_date is not None
        else None
    )
    print(f"Datas alteradas: {changed_dates} | mudou: {changed}")
    return ShouldCapture(
        value=changed,
        metadata={"hashes_by_date": hashes_by_date},
    )


@task(cache_policy=NO_CACHE)
def update_calendario_hashes_by_date(env: str, hashes_by_date: dict[str, str]) -> None:
    """
    Persiste no Redis os hashes da planilha processada.

    Args:
        env: Ambiente usado para selecionar a chave Redis.
        hashes_by_date: Estado atual no formato ``data ISO -> hash da linha``.
    """
    client = get_redis_client()
    key = get_calendario_redis_key(env)
    client.set(key, hashes_by_date)
    print(f"Hashes por data atualizados no Redis: {len(hashes_by_date)} datas")


@task(cache_policy=NO_CACHE)
def get_calendario_materialization_window(env: str) -> tuple[str, str, dict]:
    """
    Calcula a janela de materialização a partir das datas alteradas.

    Sem estado anterior de hashes por data, usa a janela conservadora para inicializar o estado.

    Args:
        env: Ambiente usado para recuperar o estado anterior no Redis.

    Returns:
        Data inicial, data final e variáveis adicionais da materialização.
    """
    df = get_calendario_sheet_df()
    hashes_by_date = get_calendario_hashes_by_date(df)
    previous_hashes_by_date = get_redis_client().get(get_calendario_redis_key(env))
    changed_dates = (
        get_changed_dates(previous_hashes_by_date, hashes_by_date)
        if previous_hashes_by_date is not None
        else None
    )

    today = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).strftime("%Y-%m-%d")
    if changed_dates:
        datetime_start = min(changed_dates)
        datetime_end = max(today, *changed_dates)
    else:
        max_date = df["dia"].max().strftime("%Y-%m-%d") if not df.empty else today
        datetime_start = constants.CALENDARIO_MANUAL_CUTOVER_DATE
        datetime_end = max(today, max_date, datetime_start)

    print(f"Janela de materialização: {datetime_start} → {datetime_end}")
    return datetime_start, datetime_end, {}
