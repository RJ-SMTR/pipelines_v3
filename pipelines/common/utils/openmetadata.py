# -*- coding: utf-8 -*-
"""Publicação de artefatos dbt no OpenMetadata."""

import json
import os
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

from google.cloud import storage

from pipelines.common import constants

CLI_PATH = "/opt/openmetadata/bin/metadata"
CONFIG_DIR = Path(__file__).parents[1] / "config" / "openmetadata"
CONFIG_PATH = CONFIG_DIR / "dbt_run_results.yaml"
DOCS_CONFIG_PATH = CONFIG_DIR / "dbt_docs.yaml"
GCS_PREFIX = "openmetadata/dbt"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"{path} não contém um objeto JSON")
    return value


def _write_json(path: Path, value: object) -> None:
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")
    temporary_path.replace(path)


def capture_dbt_run_results_id(target_path: Path) -> Optional[str]:
    """Retorna o ``invocation_id`` atual, se houver um resultado dbt válido."""
    try:
        invocation_id = (
            _load_json(Path(target_path) / "run_results.json")
            .get("metadata", {})
            .get("invocation_id")
        )
        return str(invocation_id) if invocation_id else None
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


def preserve_dbt_run_results(
    target_path: Path,
    previous_id: Optional[str],
    flow_run_id: object,
) -> Optional[Path]:
    """Preserva um resultado novo sem permitir que observabilidade falhe o dbt."""
    try:
        source_path = Path(target_path) / "run_results.json"
        invocation_id = _load_json(source_path).get("metadata", {}).get("invocation_id")
        if not invocation_id or str(invocation_id) == previous_id:
            print("OpenMetadata: run_results.json ausente ou inalterado; artefato ignorado")
            return None

        invocation_id = str(invocation_id)
        bundle_dir = Path(target_path) / "openmetadata" / str(flow_run_id)
        bundle_dir.mkdir(parents=True, exist_ok=True)
        index_path = bundle_dir / "index.json"
        index = _load_json(index_path) if index_path.exists() else {"version": 1, "entries": []}
        if any(entry["invocation_id"] == invocation_id for entry in index["entries"]):
            return None

        sequence = len(index["entries"]) + 1
        filename = f"run_results_{sequence:04d}_{invocation_id}.json"
        destination = bundle_dir / filename
        temporary_destination = destination.with_suffix(".json.tmp")
        shutil.copyfile(source_path, temporary_destination)
        temporary_destination.replace(destination)

        index["flow_run_id"] = str(flow_run_id)
        index["entries"].append(
            {
                "sequence": sequence,
                "invocation_id": invocation_id,
                "captured_at": _now(),
                "filename": filename,
            }
        )
        _write_json(index_path, index)
        print(f"OpenMetadata: artefato preservado em {destination}")
        return destination
    except Exception as error:
        print(f"OpenMetadata: falha não fatal ao preservar run_results.json: {error}")
        return None


def _sanitize(value: Optional[str]) -> str:
    value = value or ""
    for token in (os.getenv("OPENMETADATA_JWT_TOKEN"), os.getenv("bot_jwt_token")):
        if token:
            value = value.replace(token, "[REDACTED]")
    return value[-4000:]


def _run_cli(manifest_path: Path, run_results_path: Path) -> dict:
    started_at = time.monotonic()
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
        status = "succeeded" if result.returncode == 0 else "failed"
        response = {"status": status, "returncode": result.returncode}
        if result.returncode:
            response["output"] = _sanitize(f"{result.stdout}\n{result.stderr}")
    except subprocess.TimeoutExpired as error:
        response = {
            "status": "timed_out",
            "output": _sanitize(f"{error.stdout or ''}\n{error.stderr or ''}"),
        }
    except (OSError, ValueError) as error:
        response = {"status": "failed", "output": _sanitize(str(error))}

    response.update(
        {
            "run_results": run_results_path.name,
            "duration_seconds": round(time.monotonic() - started_at, 3),
        }
    )
    return response


def _upload_bundle(
    bundle_dir: Path,
    env: str,
    deployment_name: str,
    flow_run_id: object,
    direct_results: list[dict],
) -> dict:
    remote_prefix = f"{GCS_PREFIX}/pending/{deployment_name}/{flow_run_id}"
    _write_json(
        bundle_dir / "fallback.json",
        {"created_at": _now(), "direct_results": direct_results},
    )

    bucket_name = os.getenv("OPENMETADATA_GCS_BUCKET", constants.DEFAULT_BUCKET_NAME[env])
    client = storage.Client(project=constants.PROJECT_NAME[env])
    bucket = client.bucket(bucket_name)
    uploaded = []
    for path in sorted(bundle_dir.glob("*.json")):
        if path.name == "ready.json":
            continue
        blob_name = f"{remote_prefix}/{path.name}"
        blob = bucket.blob(blob_name)
        if not blob.exists(client=client):
            blob.upload_from_filename(
                str(path), content_type="application/json", if_generation_match=0
            )
        uploaded.append(blob_name)

    ready_path = bundle_dir / "ready.json"
    _write_json(ready_path, {"ready_at": _now(), "files": uploaded})
    ready_blob = bucket.blob(f"{remote_prefix}/ready.json")
    if not ready_blob.exists(client=client):
        ready_blob.upload_from_filename(
            str(ready_path), content_type="application/json", if_generation_match=0
        )
    return {"status": "queued", "bucket": bucket_name, "prefix": remote_prefix}


def ingest_dbt_artifacts(
    target_path: Path,
    env: str,
    deployment_name: str,
    flow_run_id: object,
) -> dict:
    """Tenta a ingestão direta e envia o bundle ao GCS em caso de falha."""
    started_at = time.monotonic()
    try:
        bundle_dir = Path(target_path) / "openmetadata" / str(flow_run_id)
        index_path = bundle_dir / "index.json"
        if index_path.exists():
            manifest_path = bundle_dir / "manifest.json"
            shutil.copyfile(Path(target_path) / "manifest.json", manifest_path)
            entries = sorted(_load_json(index_path)["entries"], key=lambda entry: entry["sequence"])
            direct_results = [
                _run_cli(manifest_path, bundle_dir / entry["filename"]) for entry in entries
            ]
            failed = any(result["status"] != "succeeded" for result in direct_results)
            response = {
                "status": "failed" if failed else "succeeded",
                "results": direct_results,
            }
            if failed:
                response["fallback"] = _upload_bundle(
                    bundle_dir, env, deployment_name, flow_run_id, direct_results
                )
        else:
            response = {"status": "skipped", "reason": "no_run_results"}
    except Exception as error:
        response = {"status": "failed", "reason": _sanitize(str(error))}

    response["total_duration_seconds"] = round(time.monotonic() - started_at, 3)
    print(f"OpenMetadata: ingestão status={response['status']}")
    return response
