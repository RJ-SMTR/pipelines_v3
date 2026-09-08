# -*- coding: utf-8 -*-
"""Funções auxiliares para a captura de dados do INMET."""

from datetime import date, datetime, timedelta

from pipelines.capture__inmet_temperatura import constants


def get_inmet_capture_window(timestamp: datetime) -> tuple[date, date]:
    """Retorna a janela de dados correspondente à data agendada."""

    current_date = timestamp.date()
    if current_date.day == constants.INMET_FIRST_HALF_SCHEDULE_DAY:
        return current_date.replace(day=1), current_date.replace(day=15)

    if current_date.day == constants.INMET_SECOND_HALF_SCHEDULE_DAY:
        previous_month_end = current_date.replace(day=1) - timedelta(days=1)
        return previous_month_end.replace(day=16), previous_month_end

    return current_date - timedelta(days=1), current_date


def split_date_range(
    start_date: date,
    end_date: date,
    max_days_per_request: int = constants.INMET_MAX_DAYS_PER_REQUEST,
) -> list[tuple[date, date]]:
    """Divide uma janela inclusiva em intervalos menores para a API do INMET."""

    if max_days_per_request <= 0:
        raise ValueError("max_days_per_request deve ser maior que zero")
    if start_date > end_date:
        raise ValueError("start_date deve ser menor ou igual a end_date")

    date_ranges = []
    current_start = start_date
    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=max_days_per_request - 1),
            end_date,
        )
        date_ranges.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return date_ranges
