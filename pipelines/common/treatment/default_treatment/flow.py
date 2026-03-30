# -*- coding: utf-8 -*-
from datetime import time
from typing import Optional

from prefect import runtime, unmapped
from prefect.tasks import Task

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    create_materialization_contexts,
    run_dbt_selector_tests,
    run_dbt_selectors,
    run_dbt_snapshots,
    save_materialization_datetime_redis,
    task_dbt_selector_test_notify_discord,
    test_fallback_run,
    wait_data_sources,
)
from pipelines.common.treatment.default_treatment.utils import DBTSelector


def create_materialization_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    selectors: list[DBTSelector],
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
    snapshot_selector: Optional[DBTSelector] = None,
    fallback_run: bool = False,
):
    """
    Cria o conjunto padrão de tasks para um fluxo de materialização.

    Args:
        env (Optional[str]): prod ou dev.
        selectors (list[DBTSelector]): Lista de selectors a serem materializadas.
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
        snapshot_selector (Optional[DBTSelector]): Selector para snapshot.
        fallback_run (bool): Indica se a execução deve ser pulada caso o selector esteja em dia.

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

    # initialize sentry for error capturing
    tasks["initialize_sentry"] = initialize_sentry(env=env)

    tasks["timestamp"] = get_scheduled_timestamp(
        wait_for=[
            tasks["initialize_sentry"],
            *tasks_wait_for.get("timestamp", []),
        ]
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
        snapshot_selector=snapshot_selector,
        wait_for=[
            tasks["setup_enviroment"],
            *tasks_wait_for.get("contexts", []),
        ],
    )

    tasks["fallback_run"] = test_fallback_run(
        contexts=tasks["contexts"],
        fallback_run=fallback_run,
        wait_for=tasks_wait_for.get("fallback_run"),
    )

    contexts, should_run = tasks["fallback_run"]

    if should_run:
        wait_data_sources_future = wait_data_sources.map(
            context=contexts,
            skip=unmapped(skip_source_check),
            wait_for=unmapped(tasks_wait_for.get("wait_data_sources")),
        )

        tasks["wait_data_sources"] = wait_data_sources_future.result()

        tasks["pre_tests"] = run_dbt_selector_tests(
            contexts=contexts,
            mode="pre",
            wait_for=[
                tasks["wait_data_sources"],
                *tasks_wait_for.get("pre_tests", []),
            ],
        )

        pre_tests_notify_discord_future = task_dbt_selector_test_notify_discord.map(
            context=contexts,
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

        tasks["pre_tests_notify_discord"] = pre_tests_notify_discord_future.result()

        tasks["run_dbt"] = run_dbt_selectors(
            contexts=contexts,
            flags=flags,
            wait_for=[
                tasks["pre_tests_notify_discord"],
                *tasks_wait_for.get("run_dbt", []),
            ],
        )

        tasks["post_tests"] = run_dbt_selector_tests(
            contexts=contexts,
            mode="post",
            wait_for=[
                tasks["run_dbt"],
                *tasks_wait_for.get("post_tests", []),
            ],
        )

        post_tests_notify_discord_future = task_dbt_selector_test_notify_discord.map(
            context=contexts,
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

        tasks["post_tests_notify_discord"] = post_tests_notify_discord_future.result()

        tasks["run_dbt_snapshots"] = run_dbt_snapshots(
            contexts=contexts,
            flags=flags,
            wait_for=[
                tasks["post_tests_notify_discord"],
                *tasks_wait_for.get("run_dbt_snapshots", []),
            ],
        )

        tasks["save_redis"] = save_materialization_datetime_redis(
            contexts=contexts,
            wait_for=[tasks["run_dbt_snapshots"], *tasks_wait_for.get("save_redis", [])],
        )

    return tasks
