# -*- coding: utf-8 -*-
from typing import Optional

from prefect import runtime
from prefect.tasks import Task

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_quality_check.tasks import (
    get_quality_check_datetime_end,
    get_quality_check_datetime_start,
    set_redis_quality_check_datetime,
    task_dbt_test_notify_discord,
    task_run_dbt_tests,
)
from pipelines.common.treatment.default_treatment.utils import DBTTest


def create_quality_check_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    dbt_test: DBTTest,
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    partitions: Optional[list[str]],
    webhook_key: str = "dataplex",
    additional_mentions: Optional[list[str]] = None,
    tasks_wait_for: Optional[dict[str, list[Task]]] = None,
):
    """
    Cria o conjunto padrão de tasks para um fluxo de teste de qualidade de dados.

    Args:
        env (Optional[str]): prod ou dev.
        dbt_test (DBTTest): Objeto representando o teste do DBT.
        datetime_start (Optional[str]): Data/hora inicial para recorte dos dados.
        datetime_end (Optional[str]): Data/hora final para recorte dos dados.
        partitions (Optional[list[str]]): Lista de partições para execução dos testes.
        webhook_key (str): Chave do webhook para notificações dos testes no Discord.
        additional_mentions (Optional[list[str]]): Menções adicionais a serem incluídas
            nas notificações dos testes no Discord.
        tasks_wait_for (Optional[dict[str, list[Task]]]): Mapeamento para adicionar tasks no
            argumento wait_for das tasks retornadas por esta função.

    Returns:
        dict: Dicionário com o retorno das tasks.
    """
    tasks = {}
    tasks_wait_for = tasks_wait_for or {}

    deployment_name = runtime.deployment.name

    tasks["env"] = get_run_env(
        env=env,
        deployment_name=deployment_name,
        wait_for=tasks_wait_for.get("env"),
    )

    tasks["initialize_sentry"] = initialize_sentry(env=env)

    tasks["setup_enviroment"] = setup_environment(
        env=env,
        wait_for=[
            tasks["initialize_sentry"],
            *tasks_wait_for.get("setup_enviroment", []),
        ],
    )

    tasks["timestamp"] = get_scheduled_timestamp(
        wait_for=[
            tasks["initialize_sentry"],
            *tasks_wait_for.get("timestamp", []),
        ],
    )

    tasks["datetime_start"] = get_quality_check_datetime_start(
        env=env,
        dbt_test=dbt_test,
        datetime_start=datetime_start,
        partitions=partitions,
        wait_for=tasks_wait_for.get("datetime_start"),
    )

    tasks["datetime_end"] = get_quality_check_datetime_end(
        timestamp=tasks["timestamp"],
        datetime_start=tasks["datetime_start"],
        datetime_end=datetime_end,
        partitions=partitions,
        wait_for=tasks_wait_for.get("datetime_end"),
    )

    tasks["run_dbt_tests"] = task_run_dbt_tests(
        dbt_test=dbt_test,
        datetime_start=tasks["datetime_start"],
        datetime_end=tasks["datetime_end"],
        partitions=partitions,
        wait_for=tasks_wait_for.get("run_dbt_tests"),
    )

    tasks["notify_discord"] = task_dbt_test_notify_discord(
        dbt_test=dbt_test,
        dbt_vars=tasks["run_dbt_tests"][1],
        dbt_logs=tasks["run_dbt_tests"][0],
        webhook_key=webhook_key,
        additional_mentions=additional_mentions,
        wait_for=tasks_wait_for.get("notify_discord"),
    )

    tasks["save_redis"] = set_redis_quality_check_datetime(
        env=env,
        dbt_test=dbt_test,
        datetime_end=tasks["datetime_end"],
        wait_for=[tasks["run_dbt_tests"], *tasks_wait_for.get("save_redis", [])],
    )

    return tasks
