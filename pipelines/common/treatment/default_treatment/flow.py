# -*- coding: utf-8 -*-
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
    install_dbt_packages,
    run_dbt_selector_tests,
    run_dbt_selectors,
    run_dbt_snapshots,
    save_materialization_datetime_redis,
    setup_dbt_queries,
    task_dbt_selector_test_notify_discord,
    test_fallback_run,
    wait_data_sources,
)
from pipelines.common.treatment.default_treatment.utils import (
    MATERIALIZATION_RUN_MODES,
    DBTSelector,
    MaterializationTestConfig,
)


def create_materialization_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    selectors: list[DBTSelector],
    datetime_start: Optional[str],
    datetime_end: Optional[str],
    run_mode: str = "full",
    skip_source_check: bool = False,
    fallback_run: bool = False,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict[str, str]] = None,
    test_config: Optional[MaterializationTestConfig] = None,
    snapshot_selector: Optional[DBTSelector] = None,
    tasks_wait_for: Optional[dict[str, list[Task]]] = None,
):
    """
    Cria o conjunto padrão de tasks para um fluxo de materialização.

    Args:
        env (Optional[str]): prod ou dev.
        selectors (list[DBTSelector]): Lista de selectors a serem materializadas.
        datetime_start (Optional[str]): Data/hora inicial para recorte dos dados.
        datetime_end (Optional[str]): Data/hora final para recorte dos dados.
        run_mode (str): Nome de um modo de execução em `MATERIALIZATION_RUN_MODES`:
            - "full": pre_test, materialização, post_test e snapshots.
            - "skip_pre_test": materialização, post_test e snapshots, sem pre_test.
            - "test_only": apenas pre_test e post_test.
            - "post_test_only": apenas post_test.
        skip_source_check (bool): Indica se a checagem das fontes de dados deve ser ignorada.
        fallback_run (bool): Indica se a execução deve ser pulada caso o selector esteja em dia.
        flags (Optional[list[str]]): Flags adicionais para execução do dbt.
        additional_vars (Optional[dict[str, str]]): Variáveis DBT adicionais.
        test_config (Optional[MaterializationTestConfig]): Configuração dos testes do dbt
            (horário agendado, execução forçada e notificações no Discord).
        snapshot_selector (Optional[DBTSelector]): Selector para snapshot.
        tasks_wait_for (Optional[dict[str, list[Task]]]): Mapeamento para adicionar tasks no
            argumento wait_for das tasks retornadas por esta função.

    Returns:
        dict: Dicionário com o retorno das tasks.
    """
    if run_mode not in MATERIALIZATION_RUN_MODES:
        raise ValueError(
            f"run_mode inválido: {run_mode!r}. Modos válidos: {sorted(MATERIALIZATION_RUN_MODES)}"
        )

    mode = MATERIALIZATION_RUN_MODES[run_mode]
    test_config = test_config or MaterializationTestConfig()

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

    tasks["setup_dbt_queries"] = setup_dbt_queries(
        env=tasks["env"],
        wait_for=[tasks["setup_enviroment"]],
    )

    tasks["install_dbt_packages"] = install_dbt_packages(
        wait_for=[tasks["setup_dbt_queries"]],
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
        run_mode=run_mode,
        test_config=test_config,
        snapshot_selector=snapshot_selector,
        wait_for=[
            tasks["install_dbt_packages"],
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
            flags=flags,
            wait_for=[
                tasks["wait_data_sources"],
                *tasks_wait_for.get("pre_tests", []),
            ],
        )

        pre_tests_notify_discord_future = task_dbt_selector_test_notify_discord.map(
            context=contexts,
            mode=unmapped("pre"),
            webhook_key=unmapped(test_config.webhook_key),
            additional_mentions=unmapped(test_config.additional_mentions),
            wait_for=unmapped(
                [
                    tasks["pre_tests"],
                    *tasks_wait_for.get("pre_tests_notify_discord", []),
                ]
            ),
        )

        tasks["pre_tests_notify_discord"] = pre_tests_notify_discord_future.result()

        if mode.materialize:
            tasks["run_dbt"] = run_dbt_selectors(
                contexts=contexts,
                flags=flags,
                wait_for=[
                    tasks["pre_tests_notify_discord"],
                    *tasks_wait_for.get("run_dbt", []),
                ],
            )
        else:
            tasks["run_dbt"] = None

        tasks["post_tests"] = run_dbt_selector_tests(
            contexts=contexts,
            mode="post",
            flags=flags,
            wait_for=[
                tasks["run_dbt"],
                *tasks_wait_for.get("post_tests", []),
            ],
        )

        post_tests_notify_discord_future = task_dbt_selector_test_notify_discord.map(
            context=contexts,
            mode=unmapped("post"),
            webhook_key=unmapped(test_config.webhook_key),
            additional_mentions=unmapped(test_config.additional_mentions),
            wait_for=unmapped(
                [
                    tasks["post_tests"],
                    *tasks_wait_for.get("post_tests_notify_discord", []),
                ]
            ),
        )

        tasks["post_tests_notify_discord"] = post_tests_notify_discord_future.result()

        if mode.materialize:
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
