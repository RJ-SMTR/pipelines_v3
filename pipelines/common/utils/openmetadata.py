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

CLI_PATH = "/opt/openmetadata/bin/metadata"
CONFIG_DIR = Path(__file__).parents[1] / "config" / "openmetadata"
CONFIG_PATH = CONFIG_DIR / "dbt_run_results.yaml"
DOCS_CONFIG_PATH = CONFIG_DIR / "dbt_docs.yaml"
GCS_PREFIX = "openmetadata/dbt"


def preserve_dbt_run_results(
    target_path: Path,
    flow_run_id: object,
) -> Optional[Path]:
    """Preserva o resultado gerado sem permitir que observabilidade falhe o dbt."""
    try:
        source_path = Path(target_path) / "run_results.json"
        if not source_path.exists():
            print("OpenMetadata: comando dbt não gerou run_results.json")
            return None

        run_results = json.loads(source_path.read_text(encoding="utf-8"))
        invocation_id = run_results.get("metadata", {}).get("invocation_id")
        if not invocation_id:
            print("OpenMetadata: run_results.json ausente ou inválido; artefato ignorado")
            return None

        artifacts_dir = Path(target_path) / "openmetadata" / str(flow_run_id)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        sequence = len(list(artifacts_dir.glob("run_results_*.json"))) + 1
        filename = f"run_results_{sequence:04d}_{invocation_id}.json"
        destination = artifacts_dir / filename
        shutil.copyfile(source_path, destination)
        print(f"OpenMetadata: artefato preservado em {destination}")
        return destination
    except Exception as error:
        print(f"OpenMetadata: falha não fatal ao preservar run_results.json: {error}")
        return None


def _run_cli(manifest_path: Path, run_results_path: Path) -> bool:
    process_env = os.environ.copy()
    process_env["OPENMETADATA_JWT_TOKEN"] = process_env.get(
        "OPENMETADATA_JWT_TOKEN", process_env.get("bot_jwt_token", "")
    )
    process_env["OPENMETADATA_DBT_MANIFEST_PATH"] = str(manifest_path)
    process_env["OPENMETADATA_DBT_RUN_RESULTS_PATH"] = str(run_results_path)
    try:
        result = subprocess.run(
            [os.getenv("OPENMETADATA_CLI_PATH", CLI_PATH), "ingest", "-c", str(CONFIG_PATH)],
            check=False,
            capture_output=True,
            text=True,
            timeout=int(os.getenv("OPENMETADATA_TIMEOUT_SECONDS", "600")),
            env=process_env,
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
    except (OSError, ValueError) as error:
        print(f"OpenMetadata: falha ao ingerir {run_results_path.name}: {error}")
        return False

    print(f"OpenMetadata: {run_results_path.name} ingerido com sucesso")
    return True


def _upload_artifacts_to_gcs(
    artifacts_dir: Path,
    env: str,
    deployment_name: str,
    flow_run_id: object,
) -> None:
    remote_prefix = f"{GCS_PREFIX}/pending/{deployment_name}/{flow_run_id}"
    bucket_name = os.getenv("OPENMETADATA_GCS_BUCKET", constants.DEFAULT_BUCKET_NAME[env])
    client = storage.Client(project=constants.PROJECT_NAME[env])
    bucket = client.bucket(bucket_name)
    for path in sorted(artifacts_dir.glob("*.json")):
        blob_name = f"{remote_prefix}/{path.name}"
        blob = bucket.blob(blob_name)
        if not blob.exists(client=client):
            blob.upload_from_filename(
                str(path), content_type="application/json", if_generation_match=0
            )
    print(f"OpenMetadata: artefatos enviados para gs://{bucket_name}/{remote_prefix}")


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

        manifest_path = artifacts_dir / "manifest.json"
        shutil.copyfile(Path(target_path) / "manifest.json", manifest_path)
        success = True
        for run_results_path in run_results_paths:
            success = _run_cli(manifest_path, run_results_path) and success

        if not success:
            _upload_artifacts_to_gcs(artifacts_dir, env, deployment_name, flow_run_id)
        else:
            print("OpenMetadata: ingestão concluída com sucesso")
    except Exception as error:
        print(f"OpenMetadata: falha não fatal na ingestão: {error}")
