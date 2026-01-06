# -*- coding: utf-8 -*-
from datetime import datetime

from croniter import croniter


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
