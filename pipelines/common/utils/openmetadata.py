# -*- coding: utf-8 -*-
"""Utilitários para ingestão de metadados no OpenMetadata."""

import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Optional

import yaml
from google.cloud import storage

from pipelines.common import constants
from pipelines.common.utils.secret import get_env_secret

CLI_PATH = "/opt/openmetadata/bin/metadata"
CLI_PYTHON_PATH = "/opt/openmetadata/bin/python"
CLI_TIMEOUT_SECONDS = 600
CONFIG_PATH = Path(__file__).parents[1] / "config" / "openmetadata"
GCS_BUCKET_NAME = "rj-smtr"
GCS_PREFIX = "openmetadata/dbt"


def _exact_patterns(values: Iterable[str]) -> list[str]:
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
    config = yaml.safe_load((CONFIG_PATH / "dbt_run_results.yaml").read_text(encoding="utf-8"))
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


def _create_bigquery_ingestion_config(
    project_ids: list[str],
    billing_project_id: str,
    schema_filter_pattern: Optional[dict[str, list[str]]] = None,
    table_filter_pattern: Optional[dict[str, list[str]]] = None,
) -> dict:
    """Carrega e parametriza a configuração de ingestão do BigQuery."""
    config = yaml.safe_load((CONFIG_PATH / "bigquery.yaml").read_text(encoding="utf-8"))
    connection_config = config["source"]["serviceConnection"]["config"]
    source_config = config["source"]["sourceConfig"]["config"]

    connection_config["billingProjectId"] = billing_project_id
    connection_config["credentials"]["gcpConfig"]["projectId"] = project_ids
    source_config["databaseFilterPattern"] = {"includes": _exact_patterns(project_ids)}

    if schema_filter_pattern:
        source_config["schemaFilterPattern"] = schema_filter_pattern
    if table_filter_pattern:
        source_config["tableFilterPattern"] = table_filter_pattern

    return config


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


def _prepare_dbt_artifact_pair(
    manifest_path: Path,
    run_results_path: Path,
    ingestion_directory: Path,
) -> tuple[Path, Path]:
    """Prepara os artefatos e contorna o bug open-metadata/OpenMetadata#29824."""
    ingestion_directory.mkdir(parents=True, exist_ok=True)
    ingestion_manifest_path = ingestion_directory / "manifest.json"
    ingestion_run_results_path = ingestion_directory / "run_results.json"
    shutil.copyfile(manifest_path, ingestion_manifest_path)

    run_results = json.loads(run_results_path.read_text(encoding="utf-8"))
    normalized_results = 0
    for result in run_results.get("results") or []:
        if not isinstance(result, dict):
            continue

        unique_id = result.get("unique_id", "")
        status = result.get("status")
        if (
            unique_id.startswith("test.")
            and isinstance(status, str)
            and status
            and status != "success"
            and not result.get("message")
        ):
            # O OpenMetadata 1.13.3 descarta indevidamente testes executados quando
            # message=null. O valor não é persistido para resultados com sucesso.
            result["message"] = f"dbt test finished with status '{status}'"
            normalized_results += 1

    ingestion_run_results_path.write_text(
        json.dumps(run_results, ensure_ascii=False),
        encoding="utf-8",
    )
    if normalized_results:
        print(
            "OpenMetadata: workaround para message=null aplicado a "
            f"{normalized_results} resultado(s) de {run_results_path.name}"
        )

    return ingestion_manifest_path, ingestion_run_results_path


def _run_cli(
    config_path: Path,
    artifact_name: str,
    extra_environment: Optional[dict[str, str]] = None,
    timeout_seconds: int = CLI_TIMEOUT_SECONDS,
    service_name: Optional[str] = None,
) -> bool:
    """Executa uma configuração de ingestão pelo CLI isolado do OpenMetadata."""
    try:
        secret = get_env_secret("openmetadata")
        cli_env = os.environ.copy()
        cli_env.update(
            {
                "OPENMETADATA_HOST_PORT": secret["host_port"],
                "OPENMETADATA_JWT_TOKEN": secret["jwt_token"],
            }
        )
        service_name = service_name or secret.get("service_name")
        if service_name:
            cli_env["OPENMETADATA_SERVICE_NAME"] = service_name
        if extra_environment:
            cli_env.update(extra_environment)

        config_text = config_path.read_text(encoding="utf-8")
        variables = set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", config_text))
        missing_variables = sorted(variable for variable in variables if variable not in cli_env)
        if missing_variables:
            raise ValueError(
                "variáveis ausentes na configuração do OpenMetadata: "
                + ", ".join(missing_variables)
            )
        rendered_config = re.sub(
            r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}",
            lambda match: cli_env[match.group(1)],
            config_text,
        )

        # O modo isolado impede PYTHONPATH/PYTHONHOME do flow (Python 3.13) de
        # contaminarem o virtualenv do OpenMetadata (Python 3.11).
        with tempfile.TemporaryDirectory(prefix="openmetadata-config-") as temporary_directory:
            rendered_config_path = Path(temporary_directory) / config_path.name
            rendered_config_path.write_text(rendered_config, encoding="utf-8")
            result = subprocess.run(
                [CLI_PYTHON_PATH, "-I", CLI_PATH, "ingest", "-c", str(rendered_config_path)],
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env=cli_env,
            )
        if result.returncode:
            print(
                f"OpenMetadata: falha ao ingerir {artifact_name} "
                f"(código {result.returncode}):\n{result.stdout}\n{result.stderr}"
            )
            return False
    except subprocess.TimeoutExpired as error:
        print(
            f"OpenMetadata: timeout ao ingerir {artifact_name}:\n"
            f"{error.stdout or ''}\n{error.stderr or ''}"
        )
        return False
    except (KeyError, OSError, ValueError) as error:
        print(f"OpenMetadata: falha ao ingerir {artifact_name}: {error}")
        return False

    print(f"OpenMetadata: {artifact_name} ingerido com sucesso")
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
        with tempfile.TemporaryDirectory(prefix="openmetadata-ingestion-") as temporary_dir:
            temporary_path = Path(temporary_dir)
            for run_results_path in run_results_paths:
                suffix = run_results_path.name.removeprefix("run_results_")
                manifest_path = artifacts_dir / f"manifest_{suffix}"
                if not manifest_path.is_file():
                    print(
                        "OpenMetadata: manifest correspondente não encontrado para "
                        f"{run_results_path.name}"
                    )
                    failed_artifact_paths.append(run_results_path)
                    continue

                try:
                    ingestion_manifest_path, ingestion_run_results_path = (
                        _prepare_dbt_artifact_pair(
                            manifest_path,
                            run_results_path,
                            temporary_path / suffix.removesuffix(".json"),
                        )
                    )
                    config_path = _create_dbt_ingestion_config(
                        ingestion_manifest_path,
                        ingestion_run_results_path,
                    )
                except (KeyError, OSError, TypeError, ValueError, yaml.YAMLError) as error:
                    print(f"OpenMetadata: falha ao preparar {run_results_path.name}: {error}")
                    failed_artifact_paths.extend([manifest_path, run_results_path])
                    continue

                if not _run_cli(
                    config_path=config_path,
                    artifact_name=run_results_path.name,
                    extra_environment={
                        "OPENMETADATA_DBT_MANIFEST_PATH": str(ingestion_manifest_path),
                        "OPENMETADATA_DBT_RUN_RESULTS_PATH": str(ingestion_run_results_path),
                    },
                ):
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
