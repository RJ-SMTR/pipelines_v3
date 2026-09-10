# -*- coding: utf-8 -*-
"""Funções auxiliares para a captura de dados do INMET."""

from datetime import date, datetime, timedelta

from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.utils import convert_timezone

INMET_FIRST_HALF_SCHEDULE_DAY = 22
INMET_SECOND_HALF_SCHEDULE_DAY = 7


def get_inmet_capture_window(timestamp: datetime) -> tuple[date, date]:
    """Retorna a janela de dados correspondente à data agendada."""

    current_date = timestamp.date()
    if current_date.day == INMET_FIRST_HALF_SCHEDULE_DAY:
        return current_date.replace(day=1), current_date.replace(day=15)

    if current_date.day == INMET_SECOND_HALF_SCHEDULE_DAY:
        previous_month_end = current_date.replace(day=1) - timedelta(days=1)
        return previous_month_end.replace(day=16), previous_month_end

    return current_date - timedelta(days=1), current_date


class InmetSourceTable(SourceTable):
    """Expande os agendamentos do INMET em capturas diárias da quinzena."""

    def get_uncaptured_timestamps(
        self, timestamp: datetime, retroactive_days: int = 2
    ) -> list[datetime]:
        """Recaptura a quinzena inteira nos dias 7 e 22, mesmo se já houver arquivos.

        Fora desses dias, mantém a busca padrão por capturas pendentes.
        """
        timestamp = convert_timezone(timestamp)
        if timestamp.day not in (INMET_FIRST_HALF_SCHEDULE_DAY, INMET_SECOND_HALF_SCHEDULE_DAY):
            return super().get_uncaptured_timestamps(timestamp, retroactive_days)

        start_date, end_date = get_inmet_capture_window(timestamp)
        timestamps = []
        for offset in range((end_date - start_date).days + 1):
            capture_date = start_date + timedelta(days=offset)
            capture_timestamp = timestamp.replace(
                year=capture_date.year, month=capture_date.month, day=capture_date.day
            )
            if capture_timestamp >= self.first_timestamp:
                timestamps.append(capture_timestamp)

        return timestamps[: self.max_recaptures]
