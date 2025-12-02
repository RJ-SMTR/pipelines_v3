# -*- coding: utf-8 -*-
"""Modulo com tasks para uso geral"""

from datetime import datetime
from typing import Optional

from iplanrio.pipelines_utils.env import inject_bd_credentials
from prefect import runtime, task

from pipelines.common.utils.utils import convert_timezone, is_running_locally


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
def get_run_env(env: Optional[str], deployment_name: str) -> str:
    """
    Determina o ambiente de execução baseado no nome do deployment ou configuração local.

    Args:
        env (Optional[str]): prod ou dev.
        deployment_name (str): Nome do deployment usado para inferir o ambiente.

    Returns:
        str: Ambiente final resolvido ("prod" ou "dev").
    """
    if deployment_name is not None:
        env = "prod" if deployment_name.endswith("--prod") else "dev"

    if is_running_locally() and env is None:
        return "dev"

    if env not in ("prod", "dev"):
        raise ValueError("O ambiente deve ser prod ou dev")

    return env


@task
def setup_environment(env: str):
    """
    Configura o ambiente inserindo credenciais necessárias.

    Args:
        env (str): prod ou dev.
    """
    environment = env if env == "prod" else "staging"
    inject_bd_credentials(environment=environment)
