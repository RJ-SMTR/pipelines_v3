# -*- coding: utf-8 -*-
"""Funções auxiliares para geração e validação de contratos ODCS."""

import hashlib
import importlib
import os
import re
import subprocess
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import yaml
from open_data_contract_standard.model import OpenDataContractStandard

from pipelines.common.capture.data_contract.dbt_importer import import_contract_from_manifest
from pipelines.common.utils.gcp.bigquery import SourceTable

CAPTURE_METADATA_COLUMNS = frozenset({"timestamp_captura"})
DEFAULT_REPOSITORY = "RJ-SMTR/pipelines_v3"
GITHUB_API = "https://api.github.com"
REQUEST_TIMEOUT = (10, 60)
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MANIFEST = ROOT / "queries" / "target" / "manifest.json"


def contract_relative_path(source_name: str, model_name: str) -> str:
    """Retorna o caminho do contrato sem aceitar caminhos arbitrários."""
    for value in (source_name, model_name):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", value):
            raise ValueError(f"Nome inválido no caminho do contrato: {value!r}")
    return f"contracts/{source_name}/{model_name}.odcs.yaml"


def _capture_sources() -> list[SourceTable]:
    """Import capture constants and discover their configured SourceTable objects."""
    sources = []
    for constants_path in sorted((ROOT / "pipelines").glob("capture__*/constants.py")):
        module_name = ".".join(constants_path.relative_to(ROOT).with_suffix("").parts)
        module = importlib.import_module(module_name)
        for value in vars(module).values():
            candidates = (value,) if isinstance(value, SourceTable) else value
            if isinstance(candidates, (list, tuple)):
                sources.extend(source for source in candidates if isinstance(source, SourceTable))
    return sources


def _managed_contracts(
    sources: Iterable[SourceTable],
) -> dict[Path, tuple[str, tuple[str, ...], tuple[str, ...]]]:
    contracts: dict[Path, tuple[str, tuple[str, ...], tuple[str, ...]]] = {}
    for source in sources:
        if not source.validate_data_contract:
            continue
        model_name = source.data_contract_model
        source_name = source.source_name
        relative_path = Path(contract_relative_path(source_name, model_name)).relative_to(
            "contracts"
        )
        settings = (
            model_name,
            tuple(source.data_contract_ignored_columns or ()),
            tuple(source.primary_keys or ()),
        )
        previous = contracts.setdefault(relative_path, settings)
        if previous != settings:
            raise ValueError(
                f"Sources mapped to contracts/{relative_path} have inconsistent model, "
                "ignored columns, or primary keys"
            )
    return contracts


def _render_contract(
    *,
    manifest: Path,
    model_name: str,
    ignored_columns: tuple[str, ...],
    primary_keys: tuple[str, ...],
    source_name: str,
) -> str:
    contract = adapt_contract_schema(
        import_contract_from_manifest(manifest, model_name),
        ignored_columns=ignored_columns,
        primary_keys=primary_keys,
    )
    contract["id"] = f"urn:datacontract:{source_name}:{model_name}"
    contract["name"] = f"{source_name}/{model_name}"
    OpenDataContractStandard.model_validate(contract)
    return yaml.safe_dump(contract, sort_keys=False, allow_unicode=True)


def generate_contracts(
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    contracts_dir: Path = ROOT / "contracts",
    check: bool = False,
) -> list[str]:
    """Generate all managed ODCS contracts, or fail when check mode finds drift."""
    if not manifest_path.is_file():
        raise FileNotFoundError(f"dbt manifest not found at {manifest_path}; run dbt parse first")

    managed = _managed_contracts(_capture_sources())
    messages = []
    for path, (model, ignored_columns, primary_keys) in sorted(managed.items()):
        expected = _render_contract(
            manifest=manifest_path,
            model_name=model,
            ignored_columns=ignored_columns,
            primary_keys=primary_keys,
            source_name=path.parts[0],
        )
        output = contracts_dir / path
        if check:
            if not output.is_file():
                messages.append(f"missing {output}")
            elif output.read_text(encoding="utf-8") != expected:
                messages.append(f"out of date {output}")
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(expected, encoding="utf-8")
            messages.append(f"generated {output}")

    if check:
        messages.extend(
            f"unregistered contract {path}"
            for path in sorted(contracts_dir.rglob("*.odcs.yaml"))
            if path.relative_to(contracts_dir) not in managed
        )
        if messages:
            raise ValueError("Contract drift detected:\n" + "\n".join(messages))
        return ["checked-in contracts match dbt and source configuration"]

    managed_paths = set(managed)
    for output in sorted(contracts_dir.rglob("*.odcs.yaml")):
        if output.relative_to(contracts_dir) not in managed_paths:
            output.unlink()
            messages.append(f"removed obsolete {output}")
    return messages


def _get_contract_ref(env: str) -> str:
    """Resolve a referência que será fixada em um único SHA."""
    if env == "prod":
        return "master"
    branch = os.environ.get("GIT_BRANCH", "").strip()
    if branch:
        return branch
    result = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=Path(__file__).resolve().parents[4],
        capture_output=True,
        text=True,
        check=False,
    )
    branch = result.stdout.strip()
    if result.returncode or not branch:
        raise ValueError(
            "Não foi possível identificar a branch: checkout ausente ou HEAD detached."
        )
    return branch


def download_contract_snapshot(paths: list[str], env: str) -> dict[str, Any]:
    """Baixa todos os contratos a partir do mesmo commit do repositório."""
    if not paths:
        return {}
    repository = os.environ.get("DATA_CONTRACT_GITHUB_REPOSITORY", DEFAULT_REPOSITORY)
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("DATA_CONTRACT_GITHUB_REPOSITORY deve ser owner/repository.")
    with requests.Session() as session:
        session.headers.update({"Accept": "application/vnd.github+json"})
        token = os.environ.get("DATA_CONTRACT_GITHUB_TOKEN")
        if token:
            session.headers["Authorization"] = f"Bearer {token}"
        ref = _get_contract_ref(env)
        response = session.get(
            f"{GITHUB_API}/repos/{repository}/commits/{quote(ref, safe='')}",
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        sha = response.json().get("sha", "")
        if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ValueError("GitHub não retornou um SHA de commit válido para o contrato.")

        contracts = {}
        for path in sorted(set(paths)):
            response = session.get(
                f"{GITHUB_API}/repos/{repository}/contents/{quote(path, safe='/')}",
                params={"ref": sha},
                headers={"Accept": "application/vnd.github.raw+json"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            content = response.content
            contract = yaml.safe_load(content)
            if (
                not isinstance(contract, dict)
                or contract.get("kind") != "DataContract"
                or not contract.get("schema")
            ):
                raise ValueError(f"Contrato ODCS inválido no repositório: {path}")
            if contract.get("servers"):
                raise ValueError(
                    f"O contrato versionado não deve conter servidores de execução: {path}"
                )
            digest = hashlib.sha256(content).hexdigest()
            contracts[path] = {"contract": contract, "sha256": digest}
            print(f"Contrato: {repository}@{sha} {path} sha256={digest}")
    return {"repository": repository, "ref": ref, "sha": sha, "contracts": contracts}


def run_datacontract(command: list[str]) -> None:
    """Executa um comando do Data Contract CLI e propaga sua falha.

    Args:
        command (list[str]): Comando e argumentos a serem executados.

    Raises:
        RuntimeError: Se o comando terminar com código diferente de zero.
    """
    result = subprocess.run(command, capture_output=True, text=True, check=False)

    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip())

    if result.returncode:
        output = "\n".join(filter(None, [result.stdout.strip(), result.stderr.strip()]))
        raise RuntimeError(f"Data Contract CLI falhou (exit code {result.returncode}):\n{output}")


def write_contract(contract: dict[str, Any], contract_path: str | Path) -> None:
    """Salva um contrato ODCS em YAML.

    Args:
        contract (dict[str, Any]): Contrato a ser salvo.
        contract_path (str | Path): Caminho de saída.
    """
    path = Path(contract_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(contract, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def adapt_contract_schema(
    contract: dict[str, Any],
    ignored_columns: Iterable[str] | None = (),
    primary_keys: Iterable[str] | None = (),
) -> dict[str, Any]:
    """Adapta o schema importado sem adicionar um servidor de execução.

    Args:
        contract (dict[str, Any]): Contrato ODCS importado do manifest dbt.
        ignored_columns (Iterable[str]): Colunas técnicas que não existem no bruto.
        primary_keys (Iterable[str]): Chaves primárias reais do source.

    Returns:
        dict[str, Any]: Contrato normalizado sem ``server.incoming``.
    """
    ignored = CAPTURE_METADATA_COLUMNS | set(ignored_columns or ())
    keys = list(primary_keys or ())
    adapted_contract = deepcopy(contract)

    for schema in adapted_contract.get("schema", []):
        properties = schema.get("properties", [])
        schema["properties"] = [
            property_ for property_ in properties if property_.get("name") not in ignored
        ]
        required = schema.get("required")
        if isinstance(required, list):
            schema["required"] = [column for column in required if column not in ignored]

        properties_by_name = {
            property_.get("name"): property_ for property_ in schema["properties"]
        }
        missing_keys = [key for key in keys if key not in properties_by_name]
        if missing_keys:
            raise ValueError(
                f"Chaves primárias não encontradas no contrato "
                f"{schema.get('name', '<sem nome>')}: {', '.join(missing_keys)}"
            )
        for property_ in schema["properties"]:
            property_.pop("primaryKey", None)
            property_.pop("primaryKeyPosition", None)
        custom_properties = schema.get("customProperties")
        if isinstance(custom_properties, list):
            schema["customProperties"] = [
                item for item in custom_properties if item.get("property") != "primaryKey"
            ]
        for position, key in enumerate(keys, start=1):
            properties_by_name[key]["primaryKey"] = True
            properties_by_name[key]["primaryKeyPosition"] = position

    return adapted_contract


def add_local_server(
    contract: dict[str, Any],
    raw_filepath: str,
    file_format: str = "csv",
    server_name: str = "incoming",
) -> dict[str, Any]:
    """Adiciona ou substitui o servidor local usado no teste do bruto.

    Args:
        contract (dict[str, Any]): Contrato ODCS.
        raw_filepath (str): Caminho local do arquivo bruto capturado.
        file_format (str): Formato do arquivo bruto reconhecido pelo Data Contract CLI.
        server_name (str): Nome do servidor ODCS a ser atualizado.

    Returns:
        dict[str, Any]: Cópia do contrato com o servidor local configurado.
    """
    adapted_contract = deepcopy(contract)
    servers = adapted_contract.get("servers", [])
    existing_server = next(
        (server for server in servers if server.get("server") == server_name),
        None,
    )

    if existing_server is None:
        servers.append(
            {
                "server": server_name,
                "type": "local",
                "path": raw_filepath,
                "format": file_format,
            }
        )
    else:
        existing_server["path"] = raw_filepath
        existing_server.setdefault("type", "local")
        existing_server.setdefault("format", file_format)

    adapted_contract["servers"] = servers
    return adapted_contract
