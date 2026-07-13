# -*- coding: utf-8 -*-
"""
Utilidades para o flow treatment__jae_timestamps_divergentes
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from prefect import Flow


@dataclass(frozen=True)
class GapRepairSpec:
    """
    Receita de reparo para uma tabela de captura da Jaé com timestamps divergentes.

    Attributes:
        recapture_flow (Flow): Flow de recaptura da tabela.
        recapture_by_table_id (bool): Se True, o flow de recaptura recebe o parâmetro
            `source_table_ids` com as tabelas a recapturar.
        materialization_flows (tuple[Flow, ...]): Flows de materialização executados após a
            recaptura. Tabelas que compartilham o mesmo flow têm seus timestamps unidos para
            o cálculo das janelas de materialização.
        sql_treatments (tuple[str, ...]): Templates SQL de atualização executados após as
            materializações. Templates compartilhados entre tabelas são executados uma única
            vez com a união dos timestamps.
        downstream_flows (tuple[Flow, ...]): Flows executados sem parâmetros após os updates
            SQL, deduplicados entre tabelas.
    """

    recapture_flow: Flow
    recapture_by_table_id: bool = False
    materialization_flows: tuple[Flow, ...] = ()
    sql_treatments: tuple[str, ...] = ()
    downstream_flows: tuple[Flow, ...] = ()


def create_datetime_windows(timestamps: list[str], max_gap_hours: int = 2) -> list[dict]:
    """
    Agrupa timestamps em janelas contíguas de datetime.

    Timestamps com intervalo maior ou igual a `max_gap_hours` em relação ao fim da janela
    corrente iniciam uma nova janela.

    Args:
        timestamps (list[str]): Timestamps ordenados no formato ISO `YYYY-MM-DD HH:MM:SS`.
        max_gap_hours (int): Intervalo em horas que separa duas janelas.

    Returns:
        list[dict]: Janelas no formato {"datetime_start": str, "datetime_end": str}.
    """
    if not timestamps:
        return []

    windows = []
    datetime_start = datetime_end = timestamps[0]
    for ts in timestamps:
        if datetime.fromisoformat(ts) >= datetime.fromisoformat(datetime_end) + timedelta(
            hours=max_gap_hours
        ):
            windows.append({"datetime_start": datetime_start, "datetime_end": datetime_end})
            datetime_start = ts

        datetime_end = ts

    windows.append({"datetime_start": datetime_start, "datetime_end": datetime_end})
    return windows
