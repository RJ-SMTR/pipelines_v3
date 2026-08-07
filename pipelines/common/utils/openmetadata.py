# -*- coding: utf-8 -*-
"""Publicação de artefatos dbt no OpenMetadata."""

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from google.cloud import storage

from pipelines.common import constants
from pipelines.common.utils.secret import get_env_secret

CLI_PATH = "/opt/openmetadata/bin/metadata"
CLI_TIMEOUT_SECONDS = 600
CONFIG_PATH = Path(__file__).parents[1] / "config" / "openmetadata" / "dbt_run_results.yaml"
GCS_BUCKET_NAME = "rj-smtr"
GCS_PREFIX = "openmetadata/dbt"


def preserve_dbt_run_results(
    target_path: Path,
    flow_run_id: object,
) -> Optional[Path]:
    """Preserva o resultado gerado sem permitir que observabilidade falhe o dbt."""
    try:
        source_path = Path(target_path) / "run_results.json"
        manifest_source_path = Path(target_path) / "manifest.json"
        if not source_path.exists():
            print("OpenMetadata: comando dbt não gerou run_results.json")
            return None
        if not manifest_source_path.exists():
            print("OpenMetadata: comando dbt não gerou manifest.json")
            return None

        run_results = json.loads(source_path.read_text(encoding="utf-8"))
        invocation_id = run_results.get("metadata", {}).get("invocation_id")
        if not invocation_id:
            print("OpenMetadata: run_results.json ausente ou inválido; artefato ignorado")
            return None

        artifacts_dir = Path(target_path) / "openmetadata" / str(flow_run_id)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        sequence = len(list(artifacts_dir.glob("run_results_*.json"))) + 1
        artifact_suffix = f"{sequence:04d}_{invocation_id}.json"
        run_results_destination = artifacts_dir / f"run_results_{artifact_suffix}"
        manifest_destination = artifacts_dir / f"manifest_{artifact_suffix}"
        shutil.copyfile(source_path, run_results_destination)
        shutil.copyfile(manifest_source_path, manifest_destination)
        print(
            "OpenMetadata: artefatos preservados em "
            f"{run_results_destination} e {manifest_destination}"
        )
        return run_results_destination
    except Exception as error:
        print(f"OpenMetadata: falha não fatal ao preservar run_results.json: {error}")
        return None


def _run_cli(manifest_path: Path, run_results_path: Path) -> bool:
    try:
        secret = get_env_secret("openmetadata")
        cli_env = os.environ.copy()
        cli_env.update(
            {
                "OPENMETADATA_HOST_PORT": secret["host_port"],
                "OPENMETADATA_SERVICE_NAME": secret["service_name"],
                "OPENMETADATA_JWT_TOKEN": secret["jwt_token"],
                "OPENMETADATA_DBT_MANIFEST_PATH": str(manifest_path),
                "OPENMETADATA_DBT_RUN_RESULTS_PATH": str(run_results_path),
            }
        )
        result = subprocess.run(
            [CLI_PATH, "ingest", "-c", str(CONFIG_PATH)],
            check=False,
            capture_output=True,
            text=True,
            timeout=CLI_TIMEOUT_SECONDS,
            env=cli_env,
        )
        if result.returncode:
            print(
                f"OpenMetadata: falha ao ingerir {run_results_path.name} "
                f"(código {result.returncode}):\n{result.stdout}\n{result.stderr}"
            )
            return False
    except subprocess.TimeoutExpired as error:
        print(
            f"OpenMetadata: timeout ao ingerir {run_results_path.name}:\n"
            f"{error.stdout or ''}\n{error.stderr or ''}"
        )
        return False
    except (KeyError, OSError, ValueError) as error:
        print(f"OpenMetadata: falha ao ingerir {run_results_path.name}: {error}")
        return False

    print(f"OpenMetadata: {run_results_path.name} ingerido com sucesso")
    return True


def _upload_artifacts_to_gcs(
    artifact_paths: list[Path],
    env: str,
    deployment_name: str,
    flow_run_id: object,
) -> None:
    remote_prefix = f"{GCS_PREFIX}/pending/{deployment_name}/{flow_run_id}"
    client = storage.Client(project=constants.PROJECT_NAME[env])
    bucket = client.bucket(GCS_BUCKET_NAME)
    for path in artifact_paths:
        blob_name = f"{remote_prefix}/{path.name}"
        blob = bucket.blob(blob_name)
        if not blob.exists(client=client):
            blob.upload_from_filename(
                str(path), content_type="application/json", if_generation_match=0
            )
    print(f"OpenMetadata: artefatos enviados para gs://{GCS_BUCKET_NAME}/{remote_prefix}")


def ingest_dbt_artifacts(
    target_path: Path,
    env: str,
    deployment_name: str,
    flow_run_id: object,
) -> None:
    """Tenta a ingestão direta e envia os artefatos ao GCS em caso de falha."""
    try:
        artifacts_dir = Path(target_path) / "openmetadata" / str(flow_run_id)
        run_results_paths = sorted(artifacts_dir.glob("run_results_*.json"))
        if not run_results_paths:
            print("OpenMetadata: nenhum run_results.json para ingerir")
            return

        failed_artifact_paths = []
        for run_results_path in run_results_paths:
            manifest_path = run_results_path.with_name(
                run_results_path.name.replace("run_results_", "manifest_", 1)
            )
            if not manifest_path.exists():
                print(f"OpenMetadata: manifest correspondente ausente para {run_results_path.name}")
                failed_artifact_paths.append(run_results_path)
                continue
            if not _run_cli(manifest_path, run_results_path):
                failed_artifact_paths.extend([manifest_path, run_results_path])

        if failed_artifact_paths:
            _upload_artifacts_to_gcs(
                failed_artifact_paths,
                env,
                deployment_name,
                flow_run_id,
            )
        else:
            print("OpenMetadata: ingestão concluída com sucesso")
    except Exception as error:
        print(f"OpenMetadata: falha não fatal na ingestão: {error}")
