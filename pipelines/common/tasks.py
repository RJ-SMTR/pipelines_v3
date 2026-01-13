# -*- coding: utf-8 -*-
"""Modulo com tasks para uso geral"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import httpx
import pandas as pd
import pandas_gbq
from iplanrio.pipelines_utils.env import inject_bd_credentials
from prefect import runtime, task

from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.utils import async_post_request, convert_timezone, is_running_locally


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
    if not is_running_locally():
        environment = env if env == "prod" else "staging"
        inject_bd_credentials(environment=environment)


@task
async def async_api_post_request(
    url: str,
    payloads: list[dict],
    max_concurrent: int,
    headers: Optional[dict] = None,
    timeout: int = 60,
) -> list[dict]:
    """
    Envia múltiplos POSTs assíncronos com controle de concorrência.

    Args:
        url: Endpoint da API.
        payloads: Lista de payloads para enviar.
        headers: Headers HTTP.
        max_concurrent: Número máximo de requisições simultâneas.
        timeout: Timeout em segundos.

    Returns:
        list[dict]: Lista com resultados de cada requisição.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def controlled_post(client: httpx.AsyncClient, payload: dict) -> dict:
        async with semaphore:
            return await async_post_request(client, url, payload, headers, timeout)

    async with httpx.AsyncClient() as client:
        tasks = [controlled_post(client, payload) for payload in payloads]
        results = await asyncio.gather(*tasks)

    return list(results)


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

    df = pandas_gbq.read_gbq(query, project_id=project_id, use_bqstorage_api=True)

    return df.to_dict(orient="records")


@task
def save_data_to_file(
    data: Union[str, dict, list[dict], pd.DataFrame],
    path: Union[str, Path],
    filetype: str,
    csv_mode: str,
):
    """
    Salva dados em um arquivo local.

    Args:
        data (Union[str, dict, list[dict], pd.DataFrame]): Dados para salvar.
        path (Union[str, Path]): Caminho do arquivo.
        filetype (str): Tipo do arquivo.
        csv_mode (str): Modo do arquivo CSV.
    """
    save_local_file(
        filepath=str(path),
        filetype=filetype,
        data=data,
        csv_mode=csv_mode,
    )
    print(f"Dados salvos em {path}")
