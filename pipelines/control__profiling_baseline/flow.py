# -*- coding: utf-8 -*-
"""
Flow vazio instrumentado — baseline de consumo de recursos.

Mede CPU/memória/tempo de um Prefect flow "só de existir", sem operações.
Serve como referência para comparar com pipelines reais (ex.: dbt em runtime).

Common: 2026-05-19
"""

import time

from pipelines.common.utils.prefect import flow
from pipelines.common.utils.profiling import profile_resources


@flow(log_prints=True)
def control__profiling_baseline(sleep_seconds: int = 0):
    """
    Args:
        sleep_seconds: Tempo (s) ocioso após o start. Mantém o processo vivo
            para o sampler observar drift de RSS/CPU ao longo do tempo.
    """
    with profile_resources("baseline_flow"):
        print(f"Flow baseline: ocioso por {sleep_seconds}s.")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
