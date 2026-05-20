# -*- coding: utf-8 -*-
from datetime import datetime

from pipelines.common.capture.jae import constants
from pipelines.common.utils.utils import convert_timezone, is_running_locally


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


def get_jae_database_settings(database_name: str) -> dict:
    """
    Pega os dados de configuração do banco de dados da Jaé
    e trata o host, se estiver executando localmente.

    Args:
        database_name (str): Nome do banco de dados

    Returns:
        dict: Configuração definida nas constantes com o host tratado
    """
    database = constants.JAE_DATABASE_SETTINGS[database_name]
    database["host"] = (
        f"{database_name.rsplit('_', maxsplit=1)[0].replace('_', '-')}-database-replica.internal"
        if is_running_locally()
        else database["host"]
    )

    return database
