# -*- coding: utf-8 -*-
"""Modulo com tasks para uso geral"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import httpx
import pandas as pd
import pandas_gbq
import sentry_sdk
from prefect import runtime, task
from prefect.cache_policies import NO_CACHE
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment
from prefect.flows import Flow

from pipelines.common import constants
from pipelines.common.utils.discord import send_discord_message
from pipelines.common.utils.env import inject_bd_credentials
from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.prefect import FailedSubFlowError
from pipelines.common.utils.secret import get_env_secret, set_local_secrets
from pipelines.common.utils.utils import async_post_request, convert_timezone, is_running_locally


@task(cache_policy=NO_CACHE)
def initialize_sentry(env):
    if is_running_locally():
        return
    print("Inicializando Sentry SDK")
    print(f"Ambiente: {env}")
    sentry_dsn = get_env_secret("sentry", "dsn")["dsn"]
    environment = env
    sentry_sdk.init(
        dsn=sentry_dsn,
        environment=environment,
    )


@task(cache_policy=NO_CACHE)
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


@task(cache_policy=NO_CACHE)
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


@task(cache_policy=NO_CACHE)
def setup_environment(env: str):
    """
    Configura o ambiente inserindo credenciais necessárias.

    Args:
        env (str): prod ou dev.
    """
    if is_running_locally():
        set_local_secrets()
    else:
        environment = env if env == "prod" else "staging"
        inject_bd_credentials(environment=environment)


@task(cache_policy=NO_CACHE)
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
    if max_concurrent < 1:
        raise ValueError("max_concurrent must be >= 1")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def controlled_post(client: httpx.AsyncClient, payload: dict) -> dict:
        async with semaphore:
            return await async_post_request(client, url, payload, headers, timeout)

    async with httpx.AsyncClient() as client:
        tasks = [controlled_post(client, payload) for payload in payloads]
        results = await asyncio.gather(*tasks)

    return list(results)


@task(cache_policy=NO_CACHE)
def query_bq(query: str, project_id: str, params: Optional[dict] = None) -> list[dict]:
    """
    Executa uma query no BigQuery.

    Args:
        query (str): Query SQL.
        project_id (str): ID do projeto no GCP.
        params (dict, optional): Parâmetros para formatar a query.

    Returns:
        list[dict]: Resultado da query como lista de dicionários.
    """
    if params:
        query = query.format(**params)

    print(f"Query: {query}")

    df = pandas_gbq.read_gbq(query, project_id=project_id, use_bqstorage_api=True)

    return df.to_dict(orient="records")


@task(cache_policy=NO_CACHE)
def save_data_to_file(
    data: Union[str, dict, list[dict], pd.DataFrame],
    path: Union[str, Path],
    filetype: str,
    csv_mode: Optional[str] = "w",
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


@task(cache_policy=NO_CACHE)
def task_send_discord_message(message: Union[str, list[str]], webhook: str):
    """
    Task para enviar uma mensagem para um canal do Discord

    Args:
        message (Union[str, list[str]]): Mensagem a ser enviada
        webhook (str): Nome da key do webhook no secret
    """
    webhook_secret = get_env_secret(constants.WEBHOOKS_SECRET_PATH)
    webhook_url = webhook_secret[webhook]

    if isinstance(message, str):
        message = [message]
    for m in message:
        final_message = "[DEV] " + m if is_running_locally() else m

        send_discord_message(message=final_message, webhook_url=webhook_url)


@task(cache_policy=NO_CACHE)
async def run_subflow(
    env: str,
    flow: Flow,
    parameters: list[dict] | None = None,
    maximum_parallelism: int = 1,
    deployment_name: str | None = None,
) -> list[FlowRun]:
    """
    Executa um deployment como subflow.

    Args:
        env (str): Prod ou dev.
        flow (Flow): Flow do prefect para ser executado.
        parameters (Optional[list[dict]]): Lista de dicionários contendo os parâmetros de cada
            execução do deployment. Se não informado, executa uma única vez com parâmetros vazios.
        maximum_parallelism (int): Número máximo de execuções simultâneas do deployment.
        deployment_name (Optional[str]): Nome do deployment

    Returns:
        list[FlowRun]: Lista contendo os objetos `FlowRun` retornados pelo `run_deployment`.
    """

    if maximum_parallelism <= 0:
        raise ValueError("maximum_parallelism deve ser maior que 0.")

    parameters = parameters or [{}]

    flow_name = flow.name
    flow_type, pipeline = flow_name.split("--", maxsplit=1)

    flow_env = "prod" if env == "prod" else "staging"

    deployment_name = deployment_name or f"rj-{flow_type}--{pipeline.replace('-', '_')}--{flow_env}"

    deployment_name = f"{flow_name}/{deployment_name}"

    semaphore = asyncio.Semaphore(maximum_parallelism)

    async def _run(params):
        async with semaphore:
            print(f"Executando o deployment {deployment_name} com os parâmetros:\n{params}")
            return await run_deployment(
                name=deployment_name,
                parameters=params,
            )

    coroutines = [_run(params) for params in parameters]
    runs = await asyncio.gather(*coroutines)

    fail_message = "Os seguintes execuções falharam:\n"
    flow_run_base_url = "https://prefect-v3.mobilidade.rio/runs/flow-run"
    raise_error = False
    for run in runs:
        if not run.state.is_completed():
            fail_message += (
                f"\n{flow_run_base_url}/{run.id} finalizou com o estado: {run.state_name}"
            )
            raise_error = True
    if raise_error:
        raise FailedSubFlowError(fail_message)

    return runs
