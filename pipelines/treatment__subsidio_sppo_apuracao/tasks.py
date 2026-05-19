# -*- coding: utf-8 -*-
"""Tasks do flow treatment__subsidio_sppo_apuracao"""

from prefect import task
from prefect.cache_policies import NO_CACHE


class CaptureGapsDetectedError(Exception):
    """Indica que foram encontrados gaps na captura dos dados da Jaé."""


@task(cache_policy=NO_CACHE)
def raise_on_gaps(gaps_per_table: list[list[str]], table_ids: list[str]):
    """
    Bloqueia a materialização caso algum table_id apresente gaps na captura.

    Args:
        gaps_per_table (list[list[str]]): Lista de listas de timestamps com gaps,
            uma lista por table_id (mesma ordem de `table_ids`).
        table_ids (list[str]): Lista de table_ids verificados.

    Raises:
        CaptureGapsDetectedError: Se algum table_id possuir ao menos um gap.
    """
    tables_with_gaps = [
        table_id for table_id, gaps in zip(table_ids, gaps_per_table, strict=True) if len(gaps) > 0
    ]
    if tables_with_gaps:
        raise CaptureGapsDetectedError(
            f"Gaps detectados na captura JAE para: {tables_with_gaps}. Materialização abortada."
        )
    print("Sem gaps detectados na captura JAE.")
