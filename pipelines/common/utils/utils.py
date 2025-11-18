# -*- coding: utf-8 -*-
"""Funções gerais"""

import io
import os
import uuid
from datetime import date, datetime
from typing import Any

import pandas as pd
import pendulum
from croniter import croniter
from pytz import timezone

from pipelines.common import constants


def is_running_locally() -> bool:
    """
    Verifica se a execução está ocorrendo localmente.

    Returns:
        bool: True se estiver rodando localmente, False caso contrário.
    """
    return not bool(os.environ.get("RUNNING_IN_DOCKER", ""))


def custom_serialization(obj: Any) -> Any:
    """
    Função para serializar objetos não serializaveis
    pela função json.dump

    Args:
        obj (Any): Objeto a ser serializado

    Returns:
        Any: Object serializado
    """
    if isinstance(obj, (pd.Timestamp, date)):
        if isinstance(obj, pd.Timestamp):
            if obj.tzinfo is None:
                obj = obj.tz_localize("UTC").tz_convert(constants.TIMEZONE)
        return obj.isoformat()
    elif isinstance(obj, uuid.UUID):
        return str(obj)

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def data_info_str(data: pd.DataFrame):
    """
    Retorna as informações de um Dataframe como string

    Args:
        data (pd.DataFrame): Dataframe para extrair as informações

    Returns:
        str: retorno do método data.info()
    """
    buffer = io.StringIO()
    data.info(buf=buffer)
    return buffer.getvalue()


def cron_date_range(cron_expr: str, start_time: datetime, end_time: datetime) -> list[datetime]:
    """
    Gera um range de datetimes com base em uma expressão cron entre dois datetimes.

    Args:
        cron_expr (str): Expressão cron
        start_time (datetime): Datetime de início do range
        end_time (datetime): Datetime de fim do range

    Returns:
        list[datetime]: lista com o range de datetimes

    """
    iterator = croniter(cron_expr, start_time)
    current_date = iterator.get_next(datetime)
    datetimes = []

    while True:
        if current_date <= end_time:
            datetimes.append(current_date)
        else:
            break
        current_date = iterator.get_next(datetime)

    return datetimes


def cron_get_last_date(cron_expr: str, timestamp: datetime) -> datetime:
    """
    Com base em uma expressão cron, retorna a última data até um datetime de referência

    Args:
        cron_expr (str): Expressão cron
        timestamp (datetime): datetime de referência

    Returns:
        datetime: última data do cron
    """
    return croniter(cron_expr, timestamp).get_prev(datetime)


def cron_get_next_date(cron_expr: str, timestamp: datetime) -> datetime:
    """
    Com base em uma expressão cron, retorna a próxima data a partir de um datetime de referência

    Args:
        cron_expr (str): Expressão cron
        timestamp (datetime): datetime de referência

    Returns:
        datetime: próxima data do cron
    """
    return croniter(cron_expr, timestamp).get_next(datetime)


def convert_timezone(timestamp: datetime) -> datetime:
    """
    Converte um datetime para a timezone padrão definida nas constantes

    Args:
        timestamp (datetime): Datetime a ser convertido

    Returns:
        datetime: Datetime com informação de timezone
    """
    tz = timezone(constants.TIMEZONE)

    if isinstance(timestamp, pendulum.DateTime):
        pendulum_tz = timestamp.timezone
        timestamp = datetime.fromtimestamp(timestamp.timestamp(), tz=pendulum_tz)

    if timestamp.tzinfo is None:
        timestamp = tz.localize(timestamp)
    else:
        timestamp = timestamp.astimezone(tz=tz)

    return timestamp
