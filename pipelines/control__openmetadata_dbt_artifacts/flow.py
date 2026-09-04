# -*- coding: utf-8 -*-
"""Reprocessa artefatos dbt cuja ingestão no OpenMetadata falhou."""

from typing import Optional

from prefect import runtime

from pipelines.common.tasks import get_run_env, initialize_sentry, setup_environment
from pipelines.common.utils.prefect import flow, handler_notify_failure
from pipelines.control__openmetadata_dbt_artifacts.tasks import (
    delete_pending_dbt_artifacts,
    get_pending_dbt_artifacts,
    ingest_pending_dbt_artifacts,
)


@flow(
    log_prints=True,
    timeout_seconds=21600,
    on_failure=[handler_notify_failure(webhook="dataplex")],
)
def control__openmetadata_dbt_artifacts(env: Optional[str] = None) -> None:
    """Reprocessa os artefatos dbt pendentes no bucket do OpenMetadata."""
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)
    sentry = initialize_sentry(env=env)
    pending_artifacts = get_pending_dbt_artifacts(
        env=env,
        wait_for=[setup_env, sentry],
    )
    successful_artifacts = ingest_pending_dbt_artifacts(
        pending_artifacts=pending_artifacts,
        wait_for=[pending_artifacts],
    )
    delete_pending_dbt_artifacts(
        env=env,
        pending_artifacts=successful_artifacts,
        wait_for=[successful_artifacts],
    )
