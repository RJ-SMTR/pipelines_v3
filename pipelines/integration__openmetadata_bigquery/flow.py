# -*- coding: utf-8 -*-
"""Integra metadados selecionados do BigQuery ao OpenMetadata."""

from typing import Optional

from prefect import runtime

from pipelines.common.tasks import get_run_env, initialize_sentry, setup_environment
from pipelines.common.utils.prefect import flow
from pipelines.integration__openmetadata_bigquery.tasks import run_openmetadata_ingestion


@flow(log_prints=True, timeout_seconds=21600)
def integration__openmetadata_bigquery(env: Optional[str] = None) -> None:
    """Ingere metadados dos projetos BigQuery configurados."""
    env = get_run_env(env=env, deployment_name=runtime.deployment.name)
    setup_env = setup_environment(env=env)
    sentry = initialize_sentry(env=env)
    run_openmetadata_ingestion(wait_for=[setup_env, sentry])
