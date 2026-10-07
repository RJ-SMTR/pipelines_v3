# -*- coding: utf-8 -*-
"""Importa metadados dbt e Library Quality Rules de config.meta.datacontract_cli"""

import copy
from typing import Any

from datacontract.imports.dbt_importer import import_dbt_manifest


def _datacontract_meta(node: dict[str, Any]) -> dict[str, Any]:
    """
    Retorna a configuração datacontract_cli de um modelo ou coluna do manifest dbt.

    Args:
        node (dict[str, Any]): Modelo ou coluna do manifest dbt.

    Returns:
        dict[str, Any]: Configuração declarada em meta.datacontract_cli.
    """
    meta = (node.get("config") or {}).get("meta") or node.get("meta") or {}
    return meta.get("datacontract_cli") or {}


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
    rules = _datacontract_meta(node).get("quality", [])
    if not isinstance(rules, list) or any(
        not isinstance(rule, dict) or rule.get("type") != "library" for rule in rules
    ):
        raise ValueError(f"{node.get('name')}: quality deve conter Library Quality Rules")
    return copy.deepcopy(rules)


def import_contract_from_manifest(
    manifest: dict[str, Any], model: dict[str, Any]
) -> dict[str, Any]:
    """
    Importa o modelo, a chave primária e as Library Quality Rules, sem traduzir testes dbt.

    Args:
        manifest (dict[str, Any]): Manifest dbt carregado.
        model (dict[str, Any]): Node do modelo a importar.

    Returns:
        dict[str, Any]: Contrato ODCS com metadados físicos e regras de qualidade.

    Raises:
        ValueError: As regras de qualidade são incompatíveis ou a primaryKey cita
            coluna inexistente.
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
    primary_keys = _datacontract_meta(model).get("primaryKey", [])
    properties = {prop["name"]: prop for prop in schema["properties"]}
    missing = [key for key in primary_keys if key not in properties]
    if missing:
        raise ValueError(f"{model['name']}: primaryKey sem coluna: {', '.join(missing)}")
    for position, key in enumerate(primary_keys, start=1):
        properties[key]["primaryKey"] = True
        properties[key]["primaryKeyPosition"] = position

    schema["quality"] = _library_quality(model)
    for prop in schema["properties"]:
        column = model["columns"][prop["name"]]
        classification = _datacontract_meta(column).get("classification")
        if classification is not None:
            prop["classification"] = classification
        quality = _library_quality(column)
        if quality:
            prop["quality"] = quality
    return contract
