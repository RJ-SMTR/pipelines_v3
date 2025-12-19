# -*- coding: utf-8 -*-
"""Modulo com tasks para uso geral"""

import time
from datetime import datetime
from typing import Optional

import pandas_gbq
import requests
from iplanrio.pipelines_utils.env import inject_bd_credentials
from prefect import runtime, task

from pipelines.common import constants
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


@task
def api_post_request(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    data: Optional[dict] = None,
):
    """
    Faz uma requisição POST.

    Args:
        url (str): Endpoint da API.
        headers (dict, optional): Headers HTTP.
        params (dict, optional): Parâmetros da URL.
        data (dict, optional): Payload do POST.

    Returns:
        dict: JSON da resposta.
    """
    is_json = False
    if headers:
        content_type = headers.get("Content-Type", "") or headers.get("content-type", "")
        is_json = "application/json" in content_type.lower()

    request_kwargs = {
        "url": url,
        "headers": headers,
        "params": params,
        "timeout": constants.MAX_TIMEOUT_SECONDS,
    }

    if is_json:
        request_kwargs["json"] = data
    else:
        request_kwargs["data"] = data

    for retry in range(constants.MAX_RETRIES):
        response = requests.post(**request_kwargs)

        if response.ok:
            return response.json()
        if response.status_code >= 500:
            print(f"Server error {response.status_code}")
            if retry == constants.MAX_RETRIES - 1:
                response.raise_for_status()
            time.sleep(constants.RETRY_DELAY)
        else:
            response.raise_for_status()


@task
def query_bq(query: str, project_id: str) -> list[dict]:
    """
    Executa uma query no BigQuery.

    Args:
        query (str): Query SQL.
        project_id (str): ID do projeto no GCP.

    Returns:
        list[dict]: Resultado da query como lista de dicionários.
    """
    print(f"Query: {query}")

    df = pandas_gbq.read_gbq(
        query,
        project_id=project_id,
    )

    return df.to_dict(orient="records")
