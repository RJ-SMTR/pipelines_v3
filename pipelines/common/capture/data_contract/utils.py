# -*- coding: utf-8 -*-
"""Funções auxiliares para geração e validação de contratos ODCS."""

import json
import os
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import yaml
from datacontract.data_contract import DataContract
from datacontract.model.run import Run
from rich import box
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from pipelines.common.capture.data_contract.constants import (
    CAPTURE_METADATA_COLUMNS,
    DEFAULT_MANIFEST,
    GITHUB_API,
    REQUEST_TIMEOUT,
    ROOT,
)
from pipelines.common.capture.data_contract.dbt_importer import import_contract_from_manifest
from pipelines.common.utils.utils import is_running_locally


def to_field(run: Run, check: Any) -> str | None:
    """Retorna o campo qualificado quando o resultado contém vários modelos."""
    models = [item.model for item in run.checks]
    if len(set(models)) > 1:
        if check.field is None:
            return check.model
        return f"{check.model}.{check.field}"
    return check.field


def with_markup(result: Any) -> Any:
    """Aplica a cor usada pela CLI do datacontract ao status do check."""
    if result == "passed":
        return "[green]passed[/green]"
    if result == "warning":
        return "[yellow]warning[/yellow]"
    if result == "failed":
        return "[red]failed[/red]"
    if result == "error":
        return "[red]error[/red]"
    if result == "skipped":
        return "[dim blue]skipped[/dim blue]"
    return result


def _print_failed_checks(run: Run, console: Console) -> None:
    position = 1
    for check in run.checks:
        if check.result in ("passed", "skipped"):
            continue
        field = to_field(run, check)
        field = f"{field} " if field else ""
        console.print(f"{position}) {field}{check.name}: {escape(str(check.reason))}")
        if check.failedSamples:
            console.print("   Failed samples:")
            for sample in check.failedSamples:
                sample_json = json.dumps(sample, ensure_ascii=False, default=str)
                console.print(f"   - {escape(sample_json)}")
        position += 1


def print_test_results_table(run: Run, console: Console) -> None:
    """Imprime a tabela de checks usando o mesmo formato da CLI do datacontract."""
    table = Table(box=box.ROUNDED)
    table.add_column("Result", no_wrap=True)
    table.add_column("Check", max_width=100)
    table.add_column("Field", max_width=32)
    table.add_column("Details", max_width=50)
    for check in sorted(
        run.checks, key=lambda item: (item.result or "", item.model or "", item.field or "")
    ):
        table.add_row(
            with_markup(check.result),
            check.name,
            to_field(run, check),
            escape(str(check.reason)) if check.reason else None,
        )
    console.print(table)


def print_test_results_summary(run: Run, console: Console) -> None:
    """Imprime o resumo textual dos checks, sem encerrar o processo."""
    if run.result == "passed":
        skipped = sum(1 for check in run.checks if check.result == "skipped")
        skipped_info = f" ({skipped} skipped)" if skipped else ""
        console.print(
            "🟢 data contract is valid. "
            f"Run {len(run.checks)} checks{skipped_info}. "
            f"Took {(run.timestampEnd - run.timestampStart).total_seconds()} seconds."
        )
    elif run.result == "skipped":
        console.print("🔵 data contract was skipped")
    elif run.result == "warning":
        console.print("🟠 data contract has warnings. Found the following warnings:")
        _print_failed_checks(run, console)
    else:
        console.print("🔴 data contract is invalid, found the following errors:")
        _print_failed_checks(run, console)


def _github_get(
    session: requests.Session,
    repository: str,
    endpoint: str,
    **request_options: Any,
) -> requests.Response:
    """
    Consulta um endpoint da API do GitHub.

    Args:
        session (requests.Session): Sessão HTTP usada na requisição.
        repository (str): Repositório no formato `owner/name`.
        endpoint (str): Caminho do endpoint relativo ao repositório.
        request_options (Any): Opções adicionais encaminhadas para `Session.get`.

    Returns:
        requests.Response: Resposta HTTP da API do GitHub.

    Raises:
        requests.HTTPError: A resposta da API indica uma falha HTTP.
    """
    response = session.get(
        f"{GITHUB_API}/repos/{repository}/{endpoint}",
        timeout=REQUEST_TIMEOUT,
        **request_options,
    )
    response.raise_for_status()
    return response


def download_contracts_from_commit(env: str) -> Path:
    """
    Disponibiliza os contratos no diretório compartilhado do flow.

    Args:
        env (str): Ambiente usado para selecionar a branch em execução remota.

    Returns:
        Path: Diretório com os contratos ODCS.

    Raises:
        ValueError: Árvore do repositório incompleta.
        KeyError: GIT_BRANCH ausente em execução remota não produtiva.
        requests.HTTPError: Falha na consulta ao GitHub.
    """
    contracts_dir = ROOT / "contracts"
    if is_running_locally():
        return contracts_dir

    repository = "RJ-SMTR/pipelines_v3"
    ref = "master" if env == "prod" else os.environ["GIT_BRANCH"]

    with requests.Session() as session:
        session.headers.update({"Accept": "application/vnd.github+json"})
        sha = _github_get(session, repository, f"commits/{quote(ref, safe='')}").json()["sha"]

        tree = _github_get(
            session,
            repository,
            f"git/trees/{sha}",
            params={"recursive": "1"},
        ).json()
        if tree.get("truncated"):
            raise ValueError("GitHub retornou uma árvore incompleta para descobrir contratos.")
        contract_paths = [
            item["path"]
            for item in tree["tree"]
            if item["type"] == "blob"
            and item["path"].startswith("contracts/")
            and item["path"].endswith(".odcs.yaml")
        ]

        contracts_dir.mkdir(parents=True, exist_ok=True)
        for relative_path in sorted(set(contract_paths)):
            path = ROOT / relative_path
            content = _github_get(
                session,
                repository,
                f"contents/{quote(relative_path, safe='/')}",
                params={"ref": sha},
                headers={"Accept": "application/vnd.github.raw+json"},
            ).content
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    return contracts_dir


def adapt_contract_schema(
    contract: dict[str, Any],
    ignored_columns: Iterable[str] = (),
    primary_keys: Iterable[str] | None = (),
) -> dict[str, Any]:
    """
    Remove colunas ausentes no bruto e aplica as chaves primárias da fonte.

    Args:
        contract (dict[str, Any]): Contrato importado do dbt.
        ignored_columns (Iterable[str]): Colunas que não existem no bruto.
        primary_keys (Iterable[str] | None): Chaves primárias configuradas na fonte.

    Returns:
        dict[str, Any]: Cópia do contrato com colunas e chaves ajustadas.

    Raises:
        ValueError: Uma chave primária não existe entre as colunas do contrato.
    """
    ignored = CAPTURE_METADATA_COLUMNS | set(ignored_columns)
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
) -> None:
    """
    Gera contratos para modelos staging_ com datacontract_cli nos metadados.

    Args:
        manifest_path (Path): Caminho do manifest gerado pelo dbt Core.
        contracts_dir (Path): Diretório dos contratos gerados.

    Raises:
        FileNotFoundError: Manifest dbt não encontrado.
        ValueError: Contrato inválido.
    """
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Manifest dbt não encontrado em {manifest_path}; execute dbt parse primeiro"
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    generated_paths = set()
    for model in sorted(manifest["nodes"].values(), key=lambda node: node["name"]):
        if model["resource_type"] != "model" or not model["name"].startswith("staging_"):
            continue

        columns = model["columns"]
        nodes_to_check = [model]
        nodes_to_check.extend(columns.values())
        has_contract = False
        for node in nodes_to_check:
            config = node.get("config") or {}
            metadata = config.get("meta") or node.get("meta") or {}
            if "datacontract_cli" in metadata:
                has_contract = True
                break

        if not has_contract:
            continue

        model_name = model["name"]
        path = Path(model["schema"]) / f"{model_name}.odcs.yaml"
        contract = import_contract_from_manifest(manifest, model)
        contract.update(
            apiVersion="v3.1.0",
            id=f"urn:datacontract:{path.parts[0]}:{model_name}",
            name=f"{path.parts[0]}/{model_name}",
            status="active",
        )
        expected = yaml.safe_dump(contract, sort_keys=False, allow_unicode=True)
        result = DataContract(data_contract_str=expected).lint()
        if not result.has_passed():
            raise ValueError(f"Contrato inválido para {model_name}: {result.model_dump_json()}")
        output = contracts_dir / path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(expected, encoding="utf-8")
        generated_paths.add(path)
        print(f"Contrato gerado: {output}")

    for output in sorted(contracts_dir.rglob("*.odcs.yaml")):
        if output.relative_to(contracts_dir) not in generated_paths:
            output.unlink()
            print(f"Contrato obsoleto removido: {output}")
