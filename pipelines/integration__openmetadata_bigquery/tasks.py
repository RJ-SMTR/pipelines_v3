# -*- coding: utf-8 -*-
"""Tasks para ingestão de metadados do BigQuery."""

import tempfile
from pathlib import Path

import yaml
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.utils.openmetadata import (
    _create_bigquery_ingestion_config,
    _exact_patterns,
    _run_cli,
)
from pipelines.integration__openmetadata_bigquery import constants


@task(cache_policy=NO_CACHE)
def run_openmetadata_ingestion() -> None:
    """Executa a ingestão BigQuery pelo CLI isolado do OpenMetadata."""
    schema_filter_pattern = (
        {"excludes": _exact_patterns(constants.BIGQUERY_DATASET_EXCLUDES)}
        if constants.BIGQUERY_DATASET_EXCLUDES
        else None
    )
    table_filter_pattern = (
        {"excludes": _exact_patterns(constants.BIGQUERY_TABLE_EXCLUDES)}
        if constants.BIGQUERY_TABLE_EXCLUDES
        else None
    )
    config = _create_bigquery_ingestion_config(
        project_ids=constants.BIGQUERY_PROJECT_IDS,
        billing_project_id=constants.BILLING_PROJECT_ID,
        schema_filter_pattern=schema_filter_pattern,
        table_filter_pattern=table_filter_pattern,
    )

    with tempfile.TemporaryDirectory(prefix="openmetadata-bigquery-") as temporary_directory:
        config_path = Path(temporary_directory) / "bigquery.yaml"
        config_path.write_text(
            yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        success = _run_cli(
            config_path=config_path,
            artifact_name="bigquery",
            timeout_seconds=constants.OPENMETADATA_CLI_TIMEOUT_SECONDS,
            service_name=constants.OPENMETADATA_SERVICE_NAME,
        )
    if not success:
        raise RuntimeError("OpenMetadata BigQuery ingestion falhou")
    print("OpenMetadata BigQuery: ingestão concluída com sucesso")
