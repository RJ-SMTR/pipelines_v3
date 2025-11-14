# -*- coding: utf-8 -*-
"""Modulo com tasks para uso geral"""

from datetime import datetime
from typing import Optional

from prefect import runtime, task

from pipelines.utils.utils import convert_timezone


@task
def get_scheduled_timestamp(timestamp: Optional[str] = None) -> datetime:
    """
    Retorna a timestamp do agendamento da run atual

    Returns:
        datetime: A data e hora do agendamento
    """
    if timestamp is not None:
        timestamp = datetime.fromisoformat(timestamp)
    else:
        timestamp = runtime.flow_run.scheduled_start_time

    timestamp = convert_timezone(timestamp=timestamp).replace(second=0, microsecond=0)

    print(f"Created timestamp: {timestamp}")
    return timestamp


@task
def get_run_env(env: Optional[str]) -> str:
    deployment_name: str = runtime.deployment.name
    if deployment_name is not None:
        env = "prod" if deployment_name.endswith("--prod") else "dev"

    if env not in ("prod", "dev"):
        raise ValueError("O ambiente deve ser prod ou dev")
