# -*- coding: utf-8 -*-
"""
Flow de materialização das viagens do SPPO

Common 2026-05-22
"""

from typing import Optional

from prefect import runtime, unmapped

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    install_dbt_packages,
    run_dbt_selector_tests,
    run_dbt_selectors,
    run_dbt_snapshots,
    setup_dbt_queries,
    task_dbt_selector_test_notify_discord,
    test_fallback_run,
    wait_data_sources,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.common.utils.prefect import flow
from pipelines.treatment__sppo_viagens.tasks import prepare_sppo_viagens_contexts


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
)
def treatment__sppo_viagens(  # noqa: PLR0913
    env: Optional[str] = None,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    flags: Optional[list[str]] = None,
    additional_vars: Optional[dict] = None,
    fallback_run: bool = False,
    skip_source_check: bool = False,
    force_current_day: bool = False,
):
    env_task = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env_task)
    sentry_task = initialize_sentry(env=env_task)
    queries = setup_dbt_queries(env=env_task, wait_for=[setup_env])
    dbt_deps = install_dbt_packages(wait_for=[queries])

    timestamp = get_scheduled_timestamp(wait_for=[sentry_task])

    contexts = prepare_sppo_viagens_contexts(
        env=env_task,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        additional_vars=additional_vars,
        timestamp=timestamp,
        force_current_day=force_current_day,
    )

    contexts, should_run = test_fallback_run(
        contexts=contexts,
        fallback_run=fallback_run,
    )

    if should_run:
        wait_sources = wait_data_sources.map(
            context=contexts,
            skip=unmapped(skip_source_check),
        ).result()

        run_dbt = run_dbt_selectors(
            contexts=contexts,
            flags=flags,
            wait_for=[wait_sources, dbt_deps],
        )

        post_tests = run_dbt_selector_tests(
            contexts=contexts,
            mode="post",
            flags=flags,
            wait_for=[run_dbt],
        )

        post_tests_notify = task_dbt_selector_test_notify_discord.map(
            context=post_tests,
            mode=unmapped("post"),
        ).result()

        run_dbt_snapshots(
            contexts=contexts,
            flags=flags,
            wait_for=[post_tests_notify],
        )
