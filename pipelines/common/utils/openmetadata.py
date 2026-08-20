# -*- coding: utf-8 -*-
"""Publicação de artefatos dbt no OpenMetadata."""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import yaml
from google.cloud import storage

from pipelines.common import constants
from pipelines.common.utils.secret import get_env_secret

CLI_PATH = "/opt/openmetadata/bin/metadata"
CLI_TIMEOUT_SECONDS = 600
CONFIG_PATH = Path(__file__).parents[1] / "config" / "openmetadata" / "dbt_run_results.yaml"
GCS_BUCKET_NAME = "rj-smtr"
GCS_PREFIX = "openmetadata/dbt"


def _exact_patterns(values: set[str]) -> list[str]:
    return [f"^{re.escape(value)}$" for value in sorted(values)]


def _get_dbt_filter_patterns(
    manifest_path: Path,
    run_results_path: Path,
) -> dict[str, dict[str, list[str]]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_results = json.loads(run_results_path.read_text(encoding="utf-8"))
    manifest_nodes = manifest.get("nodes") or {}
    manifest_sources = manifest.get("sources") or {}

    databases = set()
    schemas = set()
    tables = set()
    for result in run_results.get("results") or []:
        if not isinstance(result, dict):
            continue

        test_id = result.get("unique_id", "")
        if not test_id.startswith("test."):
            continue

        test_node = manifest_nodes.get(test_id)
        if not isinstance(test_node, dict):
            continue

        attached_node = test_node.get("attached_node")
        if attached_node:
            entity_ids = [attached_node]
        else:
            entity_ids = (test_node.get("depends_on") or {}).get("nodes") or []

        for entity_id in entity_ids:
            entity = manifest_nodes.get(entity_id) or manifest_sources.get(entity_id)
            if not isinstance(entity, dict):
                continue

            database = entity.get("database")
            schema = entity.get("schema")
            table = entity.get("alias") or entity.get("name")
            if not all(isinstance(value, str) and value for value in (database, schema, table)):
                continue

            databases.add(database)
            schemas.add(schema)
            tables.add(table)

    if not databases or not schemas or not tables:
        raise ValueError(
            f"nenhuma tabela física encontrada para os testes de {run_results_path.name}"
        )

    return {
        "databaseFilterPattern": {"includes": _exact_patterns(databases)},
        "schemaFilterPattern": {"includes": _exact_patterns(schemas)},
        "tableFilterPattern": {"includes": _exact_patterns(tables)},
    }


def _create_dbt_ingestion_config(
    manifest_path: Path,
    run_results_path: Path,
) -> Path:
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    source_config = config["source"]["sourceConfig"]["config"]
    filter_patterns = _get_dbt_filter_patterns(manifest_path, run_results_path)
    source_config.update(filter_patterns)

    destination = run_results_path.with_name("dbt_config.yaml")
    destination.write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    databases = ", ".join(filter_patterns["databaseFilterPattern"]["includes"])
    schemas = ", ".join(filter_patterns["schemaFilterPattern"]["includes"])
    tables = ", ".join(filter_patterns["tableFilterPattern"]["includes"])
    print(
        f"OpenMetadata: filtros de {run_results_path.name}: "
        f"databases=[{databases}], schemas=[{schemas}], tables=[{tables}]"
    )
    return destination


def preserve_dbt_run_results(
    target_path: Path,
    flow_run_id: object,
) -> Optional[Path]:
    """Preserva os artefatos gerados sem permitir que observabilidade falhe o dbt."""
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

        has_test_results = any(
            isinstance(result, dict) and result.get("unique_id", "").startswith("test.")
            for result in (run_results.get("results") or [])
        )
        if not has_test_results:
            print("OpenMetadata: run_results.json sem resultados de testes; artefato ignorado")
            return None

        artifacts_dir = Path(target_path) / "openmetadata" / str(flow_run_id)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        sequence = len(list(artifacts_dir.glob("run_results_*.json"))) + 1
        filename_suffix = f"{sequence:04d}_{invocation_id}.json"
        destination = artifacts_dir / f"run_results_{filename_suffix}"
        manifest_destination = artifacts_dir / f"manifest_{filename_suffix}"
        shutil.copyfile(source_path, destination)
        shutil.copyfile(Path(target_path) / "manifest.json", manifest_destination)
        print(f"OpenMetadata: artefatos preservados em {destination} e {manifest_destination}")
        return destination
    except Exception as error:
        print(f"OpenMetadata: falha não fatal ao preservar run_results.json: {error}")
        return None


def _run_cli(manifest_path: Path, run_results_path: Path, config_path: Path) -> bool:
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
            [CLI_PATH, "ingest", "-c", str(config_path)],
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
        manifest_paths = sorted(artifacts_dir.glob("manifest_*.json"))
        run_results_paths = sorted(artifacts_dir.glob("run_results_*.json"))
        if not run_results_paths:
            print("OpenMetadata: nenhum run_results.json para ingerir")
            return

        failed_artifact_paths = []
        for manifest_path, run_results_path in zip(manifest_paths, run_results_paths, strict=True):
            try:
                config_path = _create_dbt_ingestion_config(manifest_path, run_results_path)
            except (KeyError, OSError, TypeError, ValueError, yaml.YAMLError) as error:
                print(f"OpenMetadata: falha ao preparar {run_results_path.name}: {error}")
                failed_artifact_paths.extend([manifest_path, run_results_path])
                continue

            if not _run_cli(manifest_path, run_results_path, config_path):
                failed_artifact_paths.extend([manifest_path, run_results_path])

        if not failed_artifact_paths:
            print("OpenMetadata: ingestão concluída com sucesso")
            return

        _upload_artifacts_to_gcs(
            failed_artifact_paths,
            env,
            deployment_name,
            flow_run_id,
        )
    except Exception as error:
        print(f"OpenMetadata: falha não fatal na ingestão: {error}")
