# -*- coding: utf-8 -*-
"""Importa metadados dbt e Library Quality Rules de config.meta.datacontract_cli"""

import copy
import json
from pathlib import Path
from typing import Any

from datacontract.imports.dbt_importer import import_dbt_manifest


def _library_quality(node: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extrai as Library Quality Rules dos metadados dbt.

    Args:
        node (dict[str, Any]): Modelo ou coluna do manifest dbt.

    Returns:
        list[dict[str, Any]]: Cópia das regras declaradas.

    Raises:
        ValueError: A configuração não é uma lista de Library Quality Rules.
    """
    meta = (node.get("config") or {}).get("meta") or node.get("meta") or {}
    rules = (meta.get("datacontract_cli") or {}).get("quality", [])
    if not isinstance(rules, list) or any(
        not isinstance(rule, dict) or rule.get("type") != "library" for rule in rules
    ):
        raise ValueError(f"{node.get('name')}: quality deve conter Library Quality Rules")
    return copy.deepcopy(rules)


def import_contract_from_manifest(manifest_path: str | Path, model_name: str) -> dict[str, Any]:
    """
    Importa o modelo e as Library Quality Rules, sem traduzir testes dbt.

    Args:
        manifest_path (str | Path): Caminho do manifest dbt.
        model_name (str): Nome do modelo a importar.

    Returns:
        dict[str, Any]: Contrato ODCS com metadados físicos e regras de qualidade.

    Raises:
        ValueError: Modelo ausente, ambíguo ou com regras incompatíveis.
    """
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    nodes = manifest.get("nodes") or {}
    models = [
        node
        for node in nodes.values()
        if node.get("resource_type") == "model" and node.get("name") == model_name
    ]
    if not models:
        raise ValueError(f"Modelo {model_name!r} não encontrado no manifest dbt {manifest_path}")
    if len(models) > 1:
        raise ValueError(f"Nome de modelo {model_name!r} ambíguo; use um nome único no dbt")
    model = models[0]

    # Importa tipos e restrições sem inferir chaves primárias a partir dos testes
    isolated_manifest = {"metadata": copy.deepcopy(manifest.get("metadata") or {})}
    isolated_model = copy.deepcopy(model)
    if (manifest.get("metadata") or {}).get("adapter_type") == "bigquery":
        for column in (isolated_model.get("columns") or {}).values():
            data_type = column.get("data_type")
            if isinstance(data_type, str):
                column["data_type"] = data_type.upper()
    isolated_manifest["nodes"] = {model["unique_id"]: isolated_model}
    isolated_manifest["child_map"] = {}
    imported = import_dbt_manifest(isolated_manifest, [model_name], ["model"])
    imported.apiVersion = "v3.1.0"
    contract = imported.model_dump(by_alias=True, exclude_none=True)
    schema = contract["schema"][0]
    if (model.get("config") or {}).get("materialized") == "ephemeral":
        schema.pop("physicalName", None)
    else:
        schema["physicalName"] = ".".join(model[key] for key in ("database", "schema", "alias"))

    schema["quality"] = _library_quality(model)
    for prop in schema["properties"]:
        quality = _library_quality(model["columns"][prop["name"]])
        if quality:
            prop["quality"] = quality
    return contract
