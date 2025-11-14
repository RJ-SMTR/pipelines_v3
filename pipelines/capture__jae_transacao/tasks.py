# -*- coding: utf-8 -*-
from datetime import datetime
from functools import partial
from zoneinfo import ZoneInfo

from prefect import task
from pytz import timezone

from pipelines import constants as smtr_constants
from pipelines.capture__jae_transacao import constants
from pipelines.default.capture.generic_capture.utils import SourceCaptureContext
from pipelines.utils.extractors.db import get_raw_db, get_raw_db_paginated
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone


def get_capture_delay_minutes(capture_delay_minutes: dict[str, int], timestamp: datetime) -> int:
    """
    Retorna a quantidade de minutos a ser subtraído do inicio e fim do filtro de captura
    para um determinado timestamp

    Args:
        capture_delay_minutes (dict[str, int]):
            Dicionário que mapeia timestamps em formato string ISO
            (`"%Y-%m-%d %H:%M:%S"`) para valores de delay em minutos.
            A chave `"0"` representa o primeiro delay
        timestamp (datetime):
            Timestamp de captura para o qual se deseja calcular o atraso.

    Returns:
        int: O atraso em minutos correspondente ao `timestamp`.

    Example:
        >>> capture_delay_minutes = {
        ...     "0": 5,
        ...     "2025-09-25 12:00:00": 10,
        ...     "2025-09-26 09:00:00": 15,
        ... }
        >>> get_capture_delay_minutes(capture_delay_minutes, datetime(2025, 9, 26, 10, 0))
        15
    """
    delay_timestamps = (
        convert_timezone(timestamp=datetime.fromisoformat(a))
        for a in capture_delay_minutes.keys()
        if a != "0"
    )
    delay = capture_delay_minutes["0"]
    for t in delay_timestamps:
        if timestamp >= t:
            delay = capture_delay_minutes[t.strftime("%Y-%m-%d %H:%M:%S")]

    return int(delay)


@task
def create_jae_general_extractor(context: SourceCaptureContext):
    """Cria a extração de tabelas da Jaé"""
    source = context.source
    timestamp = context.timestamp
    if source.table_id == constants.GPS_VALIDADOR_TABLE_ID and timestamp < convert_timezone(
        datetime(2025, 3, 26, 15, 31, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
    ):
        raise ValueError(
            """A recaptura de dados anteriores deve ser feita manualmente.
            A coluna de captura foi alterada de ID para data_tracking"""
        )

    credentials = get_secret(constants.JAE_SECRET_PATH)
    params = constants.JAE_TABLE_CAPTURE_PARAMS[source.table_id]

    start = source.get_last_scheduled_timestamp(timestamp=timestamp).astimezone(tz=timezone("UTC"))
    end = timestamp.astimezone(tz=timezone("UTC"))

    if source.table_id == constants.TRANSACAO_ORDEM_TABLE_ID:
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=6, minute=0, second=0)

    start = start.strftime("%Y-%m-%d %H:%M:%S")
    end = end.strftime("%Y-%m-%d %H:%M:%S")
    capture_delay_minutes = params.get("capture_delay_minutes", {"0": 0})

    delay = get_capture_delay_minutes(
        capture_delay_minutes=capture_delay_minutes, timestamp=timestamp
    )

    query = params["query"].format(
        start=start,
        end=end,
        delay=delay,
    )
    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS[database_name]
    general_func_arguments = {
        "query": query,
        "engine": database["engine"],
        "host": database["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": database_name,
        "max_retries": 3,
        "raw_filepath": context.raw_filepath,
    }
    if source.file_chunk_size is not None:
        return partial(
            get_raw_db_paginated,
            page_size=source.file_chunk_size,
            **general_func_arguments,
        )
    return partial(get_raw_db, **general_func_arguments)
