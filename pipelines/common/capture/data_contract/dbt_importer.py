# -*- coding: utf-8 -*-
"""Importa metadados dbt, testes dbt e Library Quality Rules de config.meta.datacontract_cli"""

import copy
from typing import Any

from datacontract.imports.dbt_importer import import_dbt_manifest

from pipelines.common.capture.data_contract.constants import DBT_TEST_METRICS


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


def dbt_test_quality(
    manifest: dict[str, Any], model: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """
    Converte os testes dbt not_null, unique, accepted_values e
    dbt_expectations.expect_column_values_to_match_regex do modelo em Library Quality Rules.

    O where, o error_if, o warn_if e o row_condition do teste são ignorados, pois o contrato
    valida um único arquivo bruto. A descrição vem de config.description ou, na ausência, da
    description do teste.

    Args:
        manifest (dict[str, Any]): Manifest dbt carregado.
        model (dict[str, Any]): Node do modelo a importar.

    Returns:
        dict[str, list[dict[str, Any]]]: Regras convertidas, agrupadas por coluna.

    Raises:
        ValueError: Teste de regex sem regex, ou com mostly ou flags, que o contrato não
            representa.
    """
    rules = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "test" or node.get("attached_node") != model["unique_id"]:
            continue
        test_metadata = node.get("test_metadata") or {}
        test_name = ".".join(
            part for part in (test_metadata.get("namespace"), test_metadata.get("name")) if part
        )
        metric = DBT_TEST_METRICS.get(test_name)
        if metric is None:
            continue
        config = node.get("config") or {}
        kwargs = test_metadata.get("kwargs") or {}
        rule = {"type": "library", "metric": metric, "mustBe": 0}
        if test_name == "accepted_values":
            rule["arguments"] = {"validValues": list(kwargs["values"])}
        elif test_name == "dbt_expectations.expect_column_values_to_match_regex":
            if kwargs.get("mostly", 1) != 1 or kwargs.get("flags"):
                raise ValueError(f"{node['name']}: mostly e flags não são suportados no contrato")
            if "regex" not in kwargs:
                raise ValueError(f"{node['name']}: regex ausente no teste")
            rule["arguments"] = {"pattern": kwargs["regex"]}
        description = config.get("description") or node.get("description")
        if description:
            rule["description"] = description
        if str(config.get("severity", "")).lower() == "warn":
            rule["severity"] = "warning"
        rules.setdefault(node["column_name"], []).append(rule)
    return rules


def import_contract_from_manifest(
    manifest: dict[str, Any], model: dict[str, Any]
) -> dict[str, Any]:
    """
    Importa o modelo, a chave primária, os testes dbt e as Library Quality Rules.

    Args:
        manifest (dict[str, Any]): Manifest dbt carregado.
        model (dict[str, Any]): Node do modelo a importar.

    Returns:
        dict[str, Any]: Contrato ODCS com metadados físicos e regras de qualidade.

    Raises:
        ValueError: As regras de qualidade são incompatíveis, ou a primaryKey ou um teste
            dbt cita coluna inexistente.
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
    test_rules = dbt_test_quality(manifest, model)
    missing = [column for column in test_rules if column not in properties]
    if missing:
        raise ValueError(f"{model['name']}: testes dbt sem coluna: {', '.join(missing)}")
    for prop in schema["properties"]:
        column = model["columns"][prop["name"]]
        classification = _datacontract_meta(column).get("classification")
        if classification is not None:
            prop["classification"] = classification
        quality = test_rules.get(prop["name"], []) + _library_quality(column)
        if quality:
            prop["quality"] = quality
    return contract
