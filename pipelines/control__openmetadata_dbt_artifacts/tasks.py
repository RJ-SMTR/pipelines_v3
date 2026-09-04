# -*- coding: utf-8 -*-
"""Tasks para reprocessamento de artefatos dbt pendentes no OpenMetadata."""

import tempfile
from pathlib import Path

from google.cloud import storage
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as common_constants
from pipelines.common.utils.openmetadata import (
    GCS_BUCKET_NAME,
    GCS_PREFIX,
    PendingDbtArtifact,
    ingest_dbt_artifacts,
)


@task(cache_policy=NO_CACHE, name="Get Pending dbt Artifacts")
def get_pending_dbt_artifacts(env: str) -> list[PendingDbtArtifact]:
    """Consulta no GCS os pares de artefatos dbt pendentes."""
    if env != "prod":
        print(f"OpenMetadata: consulta de pendências ignorada no ambiente {env}")
        return []

    client = storage.Client(project=common_constants.PROJECT_NAME[env])
    bucket = client.bucket(GCS_BUCKET_NAME)
    pending_prefix = f"{GCS_PREFIX}/pending/"
    blobs = list(bucket.list_blobs(prefix=pending_prefix))
    blobs_by_name = {blob.name: blob for blob in blobs}
    run_results_blobs = sorted(
        (
            blob
            for blob in blobs
            if blob.name.rsplit("/", maxsplit=1)[-1].startswith("run_results_")
            and blob.name.endswith(".json")
        ),
        key=lambda blob: blob.name,
    )

    pending_artifacts = []
    if not run_results_blobs:
        print(f"OpenMetadata: nenhum artefato pendente em gs://{GCS_BUCKET_NAME}/{pending_prefix}")
        return pending_artifacts

    artifacts_directory = Path(tempfile.mkdtemp(prefix="openmetadata-pending-artifacts-"))
    for run_results_blob in run_results_blobs:
        parent, filename = run_results_blob.name.rsplit("/", maxsplit=1)
        manifest_name = f"{parent}/manifest_{filename.removeprefix('run_results_')}"
        manifest_blob = blobs_by_name.get(manifest_name)
        if manifest_blob is None:
            raise RuntimeError(
                f"OpenMetadata: manifest correspondente não encontrado para {run_results_blob.name}"
            )

        artifact_directory = artifacts_directory / str(len(pending_artifacts))
        artifact_directory.mkdir()
        manifest_path = artifact_directory / manifest_name.rsplit("/", maxsplit=1)[-1]
        run_results_path = artifact_directory / filename
        manifest_blob.download_to_filename(str(manifest_path))
        run_results_blob.download_to_filename(str(run_results_path))

        pending_artifacts.append(
            {
                "manifest_path": str(manifest_path),
                "run_results_path": str(run_results_path),
                "manifest_blob_name": manifest_blob.name,
                "run_results_blob_name": run_results_blob.name,
                "artifacts_path": str(artifacts_directory),
            }
        )

    print(f"OpenMetadata: {len(pending_artifacts)} par(es) pendente(s) encontrado(s)")

    return pending_artifacts


@task(cache_policy=NO_CACHE, name="Ingest Pending dbt Artifacts")
def ingest_pending_dbt_artifacts(
    pending_artifacts: list[PendingDbtArtifact],
) -> list[PendingDbtArtifact]:
    """Ingere pendências e retorna somente os pares ingeridos com sucesso."""
    if not pending_artifacts:
        print("OpenMetadata: nenhum artefato dbt pendente para ingerir")
        return []

    successful_paths = ingest_dbt_artifacts(
        artifacts_path=Path(pending_artifacts[0]["artifacts_path"]),
        upload_to_gcs=False,
        timeout_seconds=7200,
    )
    successful_paths = {str(path) for path in successful_paths}
    successful_artifacts = [
        artifact
        for artifact in pending_artifacts
        if artifact["run_results_path"] in successful_paths
    ]
    print(
        f"OpenMetadata: {len(successful_artifacts)} de {len(pending_artifacts)} par(es) ingerido(s)"
    )
    return successful_artifacts


@task(cache_policy=NO_CACHE, name="Delete Ingested dbt Artifacts")
def delete_pending_dbt_artifacts(
    env: str,
    pending_artifacts: list[PendingDbtArtifact],
) -> None:
    """Remove do GCS os pares cuja ingestão terminou com sucesso."""
    if env != "prod":
        print(f"OpenMetadata: remoção de pendências ignorada no ambiente {env}")
        return
    if not pending_artifacts:
        print("OpenMetadata: nenhum artefato dbt para remover")
        return

    client = storage.Client(project=common_constants.PROJECT_NAME[env])
    bucket = client.bucket(GCS_BUCKET_NAME)
    for pending_artifact in pending_artifacts:
        bucket.blob(pending_artifact["manifest_blob_name"]).delete()
        bucket.blob(pending_artifact["run_results_blob_name"]).delete()
        print(
            f"OpenMetadata: artefatos removidos do GCS: {pending_artifact['run_results_blob_name']}"
        )
