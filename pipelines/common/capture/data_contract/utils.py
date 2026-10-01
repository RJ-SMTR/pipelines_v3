# -*- coding: utf-8 -*-
"""Funções auxiliares para geração e validação de contratos ODCS."""

import importlib
import json
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
from datacontract.data_contract import DataContract

from pipelines.common.capture.data_contract.dbt_importer import import_contract_from_manifest
from pipelines.common.utils.gcp.bigquery import SourceTable

CAPTURE_METADATA_COLUMNS = frozenset({"timestamp_captura"})
GITHUB_API = "https://api.github.com"
REQUEST_TIMEOUT = (10, 60)
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MANIFEST = ROOT / "queries" / "target" / "manifest.json"


def _capture_contracts(
    manifest_path: Path,
) -> dict[Path, tuple[str, tuple[str, ...], tuple[str, ...]]]:
    """
    Descobre os contratos configurados nas fontes de captura.

    Args:
        manifest_path (Path): Manifest dbt com os datasets resolvidos.

    Returns:
        dict: Caminhos relativos e configurações de modelo, colunas ignoradas e PKs.

    Raises:
        ValueError: Fontes do mesmo contrato possuem configurações divergentes.
    """
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    datasets = {
        node["name"]: node["schema"]
        for node in manifest["nodes"].values()
        if node.get("resource_type") == "model"
    }
    contracts = {}
    for constants_path in sorted((ROOT / "pipelines").glob("capture__*/constants.py")):
        module_name = ".".join(constants_path.relative_to(ROOT).with_suffix("").parts)
        module = importlib.import_module(module_name)
        for value in vars(module).values():
            candidates = (value,) if isinstance(value, SourceTable) else value
            if not isinstance(candidates, (list, tuple)):
                continue
            for source in candidates:
                if not isinstance(source, SourceTable) or not source.validate_data_contract:
                    continue
                path = (
                    Path(datasets[source.data_contract_model])
                    / f"{source.data_contract_model}.odcs.yaml"
                )
                settings = (
                    source.data_contract_model,
                    tuple(source.data_contract_ignored_columns or ()),
                    tuple(source.primary_keys or ()),
                )
                if contracts.setdefault(path, settings) != settings:
                    raise ValueError(f"Fontes de contracts/{path} têm configurações divergentes")
    return contracts


def _get_contract_ref(env: str) -> str:
    """
    Identifica a branch usada no download dos contratos.

    Args:
        env (str): Ambiente de execução; em produção usa master.

    Returns:
        str: Branch de produção, GIT_BRANCH ou branch do checkout local.

    Raises:
        ValueError: Não foi possível identificar a branch de desenvolvimento.
    """
    if env == "prod":
        return "master"
    branch = os.environ.get("GIT_BRANCH", "").strip()
    if branch:
        return branch
    result = subprocess.run(
        ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    branch = result.stdout.strip()
    if result.returncode or not branch:
        raise ValueError(
            "Não foi possível identificar a branch: repositório ausente ou HEAD sem branch."
        )
    return branch


def download_contract_snapshot(models: list[str] | None, env: str) -> dict[str, Any]:
    """
    Baixa os contratos de um único commit do repositório.

    Args:
        models (list[str] | None): Modelos dbt dos contratos; None descobre todos.
        env (str): Ambiente usado para selecionar a branch.

    Returns:
        dict[str, Any]: Contratos; vazio quando models é uma lista vazia.

    Raises:
        ValueError: Referência, árvore do repositório ou contrato inválido.
        requests.HTTPError: Falha na consulta ao GitHub.
    """
    if models == []:
        return {}
    repository = "RJ-SMTR/pipelines_v3"
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

        response = session.get(
            f"{GITHUB_API}/repos/{repository}/git/trees/{sha}",
            params={"recursive": "1"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        tree = response.json()
        if tree.get("truncated"):
            raise ValueError("GitHub retornou uma árvore incompleta para descobrir contratos.")
        paths = [
            item["path"]
            for item in tree["tree"]
            if item["type"] == "blob"
            and item["path"].startswith("contracts/")
            and item["path"].endswith(".odcs.yaml")
        ]
        if models is not None:
            selected = []
            for model in sorted(set(models)):
                matches = [path for path in paths if Path(path).name == f"{model}.odcs.yaml"]
                if len(matches) != 1:
                    raise ValueError(f"Esperado um contrato para {model}; encontrados: {matches}")
                selected.extend(matches)
            paths = selected

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
            contracts[path] = contract
    return contracts


def adapt_contract_schema(
    contract: dict[str, Any],
    ignored_columns: Iterable[str] | None = (),
    primary_keys: Iterable[str] | None = (),
) -> dict[str, Any]:
    """
    Remove colunas ausentes no bruto e aplica as chaves primárias da fonte.

    Args:
        contract (dict[str, Any]): Contrato importado do dbt.
        ignored_columns (Iterable[str] | None): Colunas que não existem no bruto.
        primary_keys (Iterable[str] | None): Chaves primárias configuradas na fonte.

    Returns:
        dict[str, Any]: Cópia do contrato com colunas e chaves ajustadas.

    Raises:
        ValueError: Uma chave primária não existe entre as colunas do contrato.
    """
    ignored = CAPTURE_METADATA_COLUMNS | set(ignored_columns or ())
    keys = list(primary_keys or ())
    adapted_contract = deepcopy(contract)

    for schema in adapted_contract.get("schema", []):
        properties = schema.get("properties", [])
        schema["properties"] = [
            property_ for property_ in properties if property_.get("name") not in ignored
        ]
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
    """
    Cria uma cópia do contrato para validar o arquivo bruto localmente.

    Args:
        contract (dict[str, Any]): Contrato versionado.
        raw_filepath (str): Caminho do arquivo bruto.
        file_format (str): Formato do arquivo bruto.
        server_name (str): Nome do servidor local.

    Returns:
        dict[str, Any]: Cópia com servidor local e nomes usados pelo executor.
    """
    adapted_contract = deepcopy(contract)
    for schema in adapted_contract["schema"]:
        schema["physicalName"] = schema["name"]
    adapted_contract["servers"] = [
        {"server": server_name, "type": "local", "path": raw_filepath, "format": file_format}
    ]
    return adapted_contract


def generate_contracts(
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    contracts_dir: Path = ROOT / "contracts",
) -> list[str]:
    """
    Gera os contratos ODCS a partir das fontes e do manifest dbt.

    Args:
        manifest_path (Path): Caminho do manifest gerado pelo dbt Core.
        contracts_dir (Path): Diretório dos contratos gerados.

    Returns:
        list[str]: Mensagens sobre a geração dos contratos.

    Raises:
        FileNotFoundError: Manifest dbt não encontrado.
        ValueError: Contrato inválido.
    """
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Manifest dbt não encontrado em {manifest_path}; execute dbt parse primeiro"
        )

    managed = _capture_contracts(manifest_path)
    messages = []
    for path, (model, ignored_columns, primary_keys) in sorted(managed.items()):
        contract = adapt_contract_schema(
            import_contract_from_manifest(manifest_path, model),
            ignored_columns=ignored_columns,
            primary_keys=primary_keys,
        )
        contract.update(
            id=f"urn:datacontract:{path.parts[0]}:{model}",
            name=f"{path.parts[0]}/{model}",
            status="active",
        )
        expected = yaml.safe_dump(contract, sort_keys=False, allow_unicode=True)
        result = DataContract(data_contract_str=expected).lint()
        if not result.has_passed():
            raise ValueError(f"Contrato inválido para {model}: {result.model_dump_json()}")
        output = contracts_dir / path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(expected, encoding="utf-8")
        messages.append(f"Contrato gerado: {output}")

    for output in sorted(contracts_dir.rglob("*.odcs.yaml")):
        if output.relative_to(contracts_dir) not in managed:
            output.unlink()
            messages.append(f"Contrato obsoleto removido: {output}")
    return messages
