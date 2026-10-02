# -*- coding: utf-8 -*-
"""Importa metadados dbt e Library Quality Rules de config.meta.datacontract_cli"""

import copy
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


def import_contract_from_manifest(
    manifest: dict[str, Any], model: dict[str, Any]
) -> dict[str, Any]:
    """
    Importa o modelo e as Library Quality Rules, sem traduzir testes dbt.

    Args:
        manifest (dict[str, Any]): Manifest dbt carregado.
        model (dict[str, Any]): Node do modelo a importar.

    Returns:
        dict[str, Any]: Contrato ODCS com metadados físicos e regras de qualidade.

    Raises:
        ValueError: As regras de qualidade são incompatíveis.
    """
    model = copy.deepcopy(model)
    metadata = manifest.get("metadata") or {}
    if metadata.get("adapter_type") == "bigquery":
        for column in (model.get("columns") or {}).values():
            data_type = column.get("data_type")
            if isinstance(data_type, str):
                column["data_type"] = data_type.upper()
    isolated_manifest = {"metadata": metadata, "nodes": {model["unique_id"]: model}}
    imported = import_dbt_manifest(isolated_manifest, [model["name"]], ["model"])
    contract = imported.model_dump(by_alias=True, exclude_none=True)
    schema = contract["schema"][0]
    schema["physicalName"] = ".".join(model[key] for key in ("database", "schema", "alias"))
    meta = (model.get("config") or {}).get("meta")
    primary_keys = (meta.get("datacontract_cli") or {}).get("primaryKey", [])
    properties = {prop["name"]: prop for prop in schema["properties"]}
    for position, key in enumerate(primary_keys, start=1):
        properties[key]["primaryKey"] = True
        properties[key]["primaryKeyPosition"] = position

    schema["quality"] = _library_quality(model)
    for prop in schema["properties"]:
        quality = _library_quality(model["columns"][prop["name"]])
        if quality:
            prop["quality"] = quality
    return contract
