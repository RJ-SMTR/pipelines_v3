# -*- coding: utf-8 -*-
from datetime import time
from typing import Optional

from prefect import runtime, unmapped
from prefect.tasks import Task

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    create_materialization_contexts,
    dbt_test_notify_discord,
    run_dbt_selectors,
    run_dbt_tests,
    save_materialization_datetime_redis,
    wait_data_sources,
)
from pipelines.common.utils.gcp.bigquery import SourceTable


def create_materialization_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    selectors: list[SourceTable],
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    skip_source_check: bool = False,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict[str, str]] = None,
    test_scheduled_time: Optional[time] = None,
    force_test_run: bool = False,
    test_webhook_key: str = "dataplex",
    test_additional_mentions: Optional[list[str]] = None,
    tasks_wait_for: Optional[dict[str, list[Task]]] = None,
):
    """
    Cria o conjunto padrão de tasks para um fluxo de materialização.

    Args:
        env (Optional[str]): prod ou dev.
        selectors (list[SourceTable]): Lista de selectors a serem materializadas.
        datetime_start (Optional[str]): Data/hora inicial para recorte dos dados.
        datetime_end (Optional[str]): Data/hora final para recorte dos dados.
        skip_source_check (bool): Indica se a checagem das fontes de dados deve ser ignorada.
        flags (Optional[list[str]]): Flags adicionais para execução do dbt.
        additional_vars (Optional[dict[str, str]]): Variáveis DBT adicionais.
        test_scheduled_time (Optional[time]): Horário agendado para execução dos testes.
        force_test_run (bool): Força a execução dos testes.
        test_webhook_key (str): Chave do webhook para notificações dos testes no Discord.
        test_additional_mentions (Optional[list[str]]): Menções adicionais a serem incluídas
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

    tasks["setup_enviroment"] = setup_environment(
        env=env,
        wait_for=tasks_wait_for.get("setup_enviroment"),
    )

    tasks["timestamp"] = get_scheduled_timestamp(
        wait_for=tasks_wait_for.get("timestamp"),
    )

    tasks["contexts"] = create_materialization_contexts(
        env=tasks["env"],
        selectors=selectors,
        timestamp=tasks["timestamp"],
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        additional_vars=additional_vars,
        test_scheduled_time=test_scheduled_time,
        force_test_run=force_test_run,
        wait_for=[
            tasks["setup_enviroment"],
            *tasks_wait_for.get("contexts", []),
        ],
    )

    tasks["wait_data_sources"] = wait_data_sources.map(
        context=tasks["contexts"],
        skip=unmapped(skip_source_check),
        wait_for=unmapped(tasks_wait_for.get("wait_data_sources")),
    )

    tasks["pre_tests"] = run_dbt_tests(
        contexts=tasks["contexts"],
        mode="pre",
        wait_for=[
            tasks["wait_data_sources"],
            *tasks_wait_for.get("pre_tests", []),
        ],
    )

    tasks["pre_tests_notify_discord"] = dbt_test_notify_discord.map(
        context=tasks["contexts"],
        mode=unmapped("pre"),
        webhook_key=unmapped(test_webhook_key),
        additional_mentions=unmapped(test_additional_mentions),
        wait_for=unmapped(
            [
                tasks["pre_tests"],
                *tasks_wait_for.get("pre_tests_notify_discord", []),
            ]
        ),
    )

    tasks["run_dbt"] = run_dbt_selectors(
        contexts=tasks["contexts"],
        flags=flags,
        wait_for=[
            tasks["pre_tests_notify_discord"],
            *tasks_wait_for.get("run_dbt", []),
        ],
    )

    tasks["post_tests"] = run_dbt_tests(
        contexts=tasks["contexts"],
        mode="post",
        wait_for=[
            tasks["run_dbt"],
            *tasks_wait_for.get("post_tests", []),
        ],
    )

    tasks["post_tests_notify_discord"] = dbt_test_notify_discord.map(
        context=tasks["contexts"],
        mode=unmapped("post"),
        webhook_key=unmapped(test_webhook_key),
        additional_mentions=unmapped(test_additional_mentions),
        wait_for=unmapped(
            [
                tasks["post_tests"],
                *tasks_wait_for.get("post_tests_notify_discord", []),
            ]
        ),
    )

    tasks["save_redis"] = save_materialization_datetime_redis.map(
        context=tasks["contexts"],
        wait_for=[tasks["run_dbt"], *tasks_wait_for.get("save_redis", [])],
    )

    return tasks
