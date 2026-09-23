# -*- coding: utf-8 -*-
"""Funções auxiliares para adaptar contratos ODCS ao arquivo bruto."""

import json
import os
import shutil
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

import yaml

from pipelines.common.utils.fs import get_project_root_path

CAPTURE_METADATA_COLUMNS = frozenset({"timestamp_captura"})


def get_contract_path_from_manifest(
    manifest_path: str | Path,
    model_name: str,
) -> Path:
    """Retorna o contrato ao lado do arquivo SQL localizado no manifest.

    Args:
        manifest_path (str | Path): Caminho do manifest dbt compilado.
        model_name (str): Nome do modelo dbt que representa o contrato.

    Returns:
        Path: Caminho em ``<diretório do modelo>/data_contracts``.

    Raises:
        FileNotFoundError: Se o manifest não existir.
        ValueError: Se o modelo não existir no manifest.
    """
    manifest = Path(manifest_path)
    if not manifest.is_file():
        raise FileNotFoundError(f"Manifest dbt não encontrado em {manifest}.")

    manifest_data = json.loads(manifest.read_text(encoding="utf-8"))
    model_node = next(
        (
            node
            for node in manifest_data.get("nodes", {}).values()
            if node.get("resource_type") == "model" and node.get("name") == model_name
        ),
        None,
    )
    if model_node is None:
        raise ValueError(f"Modelo dbt '{model_name}' não encontrado no manifest {manifest}.")

    relative_model_path = Path(model_node["original_file_path"]).relative_to("models")

    models_path = get_project_root_path() / "queries" / "models"
    return models_path / relative_model_path.parent / "data_contracts" / f"{model_name}.odcs.yaml"


def get_datacontract_python() -> str:
    """Resolve o Python do importer e do CLI via DATACONTRACT_PYTHON ou do processo atual."""
    command = shutil.which(os.environ.get("DATACONTRACT_PYTHON", sys.executable))
    if command is None:
        raise RuntimeError(
            "Python do Data Contract CLI não encontrado. Verifique DATACONTRACT_PYTHON."
        )
    return command


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


def load_contract(contract_path: str | Path) -> dict[str, Any]:
    """Carrega um contrato ODCS e garante o tipo esperado.

    Args:
        contract_path (str | Path): Caminho do contrato YAML.

    Returns:
        dict[str, Any]: Contrato carregado.

    Raises:
        ValueError: Se o YAML não representar um contrato ODCS.
    """
    path = Path(contract_path)
    contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise ValueError(f"Contrato ODCS inválido: {path}")
    return contract


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


def generate_contract(
    manifest_path: str | Path,
    model_name: str,
    contract_path: str | Path,
    datacontract_python: str,
) -> None:
    """Gera o ODCS pelo importer personalizado, sempre a partir do manifest dbt."""
    manifest = Path(manifest_path)
    if not manifest.is_file():
        raise FileNotFoundError(
            f"Manifest dbt não encontrado em {manifest}; execute dbt parse antes "
            "de importar o contrato."
        )
    contract = Path(contract_path)
    contract.parent.mkdir(parents=True, exist_ok=True)
    run_datacontract(
        [
            datacontract_python,
            str(Path(__file__).with_name("dbt_importer.py")),
            "--manifest",
            str(manifest),
            "--model",
            model_name,
            "--output",
            str(contract),
        ]
    )


def remove_columns_from_contract(
    contract: dict[str, Any],
    columns: Iterable[str] | None,
) -> dict[str, Any]:
    """Remove colunas técnicas das propriedades do contrato.

    Args:
        contract (dict[str, Any]): Contrato ODCS importado do manifest dbt.
        columns (Iterable[str]): Nomes das colunas que não existem no arquivo bruto.

    Returns:
        dict[str, Any]: Cópia do contrato sem as colunas informadas.
    """
    ignored_columns = set(columns or ())
    adapted_contract = deepcopy(contract)

    for schema in adapted_contract.get("schema", []):
        properties = schema.get("properties", [])
        schema["properties"] = [
            property_ for property_ in properties if property_.get("name") not in ignored_columns
        ]

        required = schema.get("required")
        if isinstance(required, list):
            schema["required"] = [column for column in required if column not in ignored_columns]

    return adapted_contract


def adjust_primary_keys_in_contract(
    contract: dict[str, Any],
    primary_keys: Iterable[str] | None,
) -> dict[str, Any]:
    """Ajusta as chaves primárias do contrato às chaves reais da captura.

    O importer pode inferir como chave um campo que possui teste ``unique``.
    Aqui a chave é apenas corrigida para refletir ``SourceTable.primary_keys``;
    o atributo ``unique`` permanece intacto porque representa uma validação
    diferente de chave primária.

    Args:
        contract (dict[str, Any]): Contrato ODCS importado do manifest dbt.
        primary_keys (Iterable[str]): Colunas que identificam o registro bruto.

    Returns:
        dict[str, Any]: Cópia com as chaves primárias reais posicionadas.

    Raises:
        ValueError: Se uma chave não estiver nas propriedades do contrato.
    """
    keys = list(primary_keys or ())
    adapted_contract = deepcopy(contract)

    for schema in adapted_contract.get("schema", []):
        properties = schema.get("properties", [])
        properties_by_name = {property_.get("name"): property_ for property_ in properties}
        missing_keys = [key for key in keys if key not in properties_by_name]
        if missing_keys:
            raise ValueError(
                f"Chaves primárias não encontradas no contrato "
                f"{schema.get('name', '<sem nome>')}: {', '.join(missing_keys)}"
            )

        for property_ in properties:
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
    adapted_contract = remove_columns_from_contract(
        contract,
        CAPTURE_METADATA_COLUMNS | set(ignored_columns or ()),
    )
    return adjust_primary_keys_in_contract(adapted_contract, primary_keys)


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


def adapt_contract_for_raw(
    contract: dict[str, Any],
    raw_filepath: str,
    file_format: str = "csv",
    ignored_columns: Iterable[str] | None = (),
    primary_keys: Iterable[str] | None = (),
) -> dict[str, Any]:
    """Prepara um contrato dbt para validar um arquivo bruto.

    ``timestamp_captura`` é adicionado pelo transform comum e, portanto, não faz
    parte do bruto. Campos técnicos específicos de uma captura podem ser passados
    em ``ignored_columns``.

    Args:
        contract (dict[str, Any]): Contrato ODCS importado do manifest dbt.
        raw_filepath (str): Caminho local do arquivo bruto capturado.
        file_format (str): Formato do arquivo bruto.
        ignored_columns (Iterable[str]): Colunas técnicas adicionais.
        primary_keys (Iterable[str]): Chaves primárias reais do source.

    Returns:
        dict[str, Any]: Contrato adaptado para a execução local do teste.
    """
    adapted_contract = adapt_contract_schema(
        contract,
        ignored_columns=ignored_columns,
        primary_keys=primary_keys,
    )

    return add_local_server(
        adapted_contract,
        raw_filepath=raw_filepath,
        file_format=file_format,
    )
