# -*- coding: utf-8 -*-
from prefect import flow, runtime, unmapped

from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
)
from pipelines.common.treatment.default_treatment.tasks import (
    run_dbt_selectors,
    run_dbt_snapshots,
    test_fallback_run,
    wait_data_sources,
)
from pipelines.common.treatment.default_treatment.utils import rename_treatment_flow_run
from pipelines.treatment__sppo_viagens.tasks import prepare_sppo_viagens_contexts


@flow(
    log_prints=True,
    flow_run_name=rename_treatment_flow_run,
)
def treatment__sppo_viagens(  # noqa: PLR0913
    env=None,
    datetime_start=None,
    datetime_end=None,
    additional_vars=None,
    fallback_run=False,
    skip_source_check=False,
):
    env_task = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_environment(env=env)
    sentry_task = initialize_sentry(env=env)

    timestamp = get_scheduled_timestamp(wait_for=[sentry_task])

    contexts = prepare_sppo_viagens_contexts(
        env=env_task,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        additional_vars=additional_vars,
        timestamp=timestamp,
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

        run_dbt_selectors(
            contexts=contexts,
            flags=None,
            wait_for=[wait_sources],
        )

        run_dbt_snapshots(
            contexts=contexts,
            flags=None,
            wait_for=[wait_sources],
        )
