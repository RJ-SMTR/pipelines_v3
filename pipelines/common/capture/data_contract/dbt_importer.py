# -*- coding: utf-8 -*-
"""Import one dbt model and its supported tests as ODCS quality rules.

The importer deliberately translates a bounded set of dbt generic tests. A test
that would lose filters or comparison semantics raises ``ValueError`` instead
of silently producing a weaker data contract.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
from pathlib import Path
from typing import Any

import yaml
from datacontract.imports.dbt_importer import import_dbt_manifest
from datacontract.imports.importer import Importer
from datacontract.imports.importer_factory import importer_factory
from open_data_contract_standard.model import OpenDataContractStandard

_RANGE_BOUND_COUNT = 2

_QUALITY_KEYS = {
    "type",
    "metric",
    "id",
    "tags",
    "businessImpact",
    "authoritativeDefinitions",
    "customProperties",
    "arguments",
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
    "severity",
    "dimension",
    "unit",
    "name",
    "description",
}
_QUALITY_OPERATORS = {
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
}


def _fail(test: dict[str, Any], reason: str) -> ValueError:
    return ValueError(
        f"Unsupported dbt test {test.get('unique_id', test.get('name', '<unknown>'))}: {reason}"
    )


def _metadata(test: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    metadata = test.get("test_metadata")
    if not isinstance(metadata, dict):
        raise _fail(test, "only generic tests with test_metadata can be imported")
    package = metadata.get("namespace") or "dbt"
    name = metadata.get("name")
    kwargs = copy.deepcopy(metadata.get("kwargs") or {})
    if not isinstance(name, str) or not isinstance(kwargs, dict):
        raise _fail(test, "malformed test_metadata")
    # dbt injects this Jinja relation argument into generic tests. It is the
    # tested model reference, not a user filter; never render or execute it.
    kwargs.pop("model", None)
    return package, name, kwargs


def _attached_tests(manifest: dict[str, Any], model: dict[str, Any]) -> list[dict[str, Any]]:
    model_id = model["unique_id"]
    nodes = manifest.get("nodes") or {}
    child_ids = set((manifest.get("child_map") or {}).get(model_id, []))
    result = []
    for unique_id, node in nodes.items():
        if node.get("resource_type") != "test":
            continue
        dependencies = set((node.get("depends_on") or {}).get("nodes") or [])
        attached_node = node.get("attached_node")
        if attached_node is not None:
            is_attached = attached_node == model_id
        else:
            is_attached = unique_id in child_ids or model_id in dependencies
        if not is_attached:
            continue
        config = node.get("config") or {}
        if config.get("enabled", True) is False:
            continue
        result.append(node)
    return result


def _check_test_config(test: dict[str, Any], kwargs: dict[str, Any]) -> None:
    config = test.get("config") or {}
    if config.get("where") not in (None, ""):
        raise _fail(test, "config.where filters are not represented by ODCS library rules")
    default_thresholds = {None, "!= 0", "> 0"}
    if config.get("limit") is not None:
        raise _fail(test, "config.limit changes the set of evaluated failures")
    if config.get("fail_calc") not in (None, "count(*)"):
        raise _fail(test, "custom config.fail_calc changes the test metric")
    for key in ("error_if", "warn_if"):
        if config.get(key) not in default_thresholds:
            raise _fail(test, f"custom config.{key} changes the test threshold")
    for key in ("where", "row_condition", "group_by"):
        if kwargs.get(key) is not None:
            raise _fail(test, f"{key} options are not represented by ODCS library rules")
    severity = str(config.get("severity", "error")).lower()
    if severity not in ("error", "warn"):
        raise _fail(test, f"unsupported severity {severity!r}")


def _severity(test: dict[str, Any]) -> str | None:
    severity = str((test.get("config") or {}).get("severity", "error")).lower()
    return "warning" if severity == "warn" else None


def _quality_rule(  # noqa: PLR0913
    metric: str,
    *,
    operator: str = "mustBe",
    value: Any = 0,
    arguments: dict[str, Any] | None = None,
    severity: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    rule: dict[str, Any] = {"type": "library", "metric": metric, operator: value}
    if arguments:
        rule["arguments"] = arguments
    if severity:
        rule["severity"] = severity
    if name:
        rule["name"] = name
    return rule


def _valid_scalar(value: Any) -> bool:
    return value is None or (
        isinstance(value, (str, int, float, bool))
        and not (isinstance(value, float) and not math.isfinite(value))
    )


def _validate_quality_arguments(
    label: str,
    metric: str,
    arguments: dict[str, Any],
    *,
    model_columns: set[str],
) -> None:
    if metric == "duplicateValues" and "properties" in arguments:
        properties = arguments["properties"]
        if (
            not isinstance(properties, list)
            or not properties
            or any(not isinstance(name, str) for name in properties)
            or len(set(properties)) != len(properties)
            or not set(properties).issubset(model_columns)
        ):
            raise ValueError(
                f"{label}: arguments.properties must be distinct existing model columns"
            )
    if metric == "invalidValues" and "validValues" in arguments:
        values = arguments["validValues"]
        if (
            not isinstance(values, list)
            or not values
            or any(value is None or not _valid_scalar(value) for value in values)
        ):
            raise ValueError(
                f"{label}: arguments.validValues must be a non-empty list of non-null scalar values"
            )
    if metric == "invalidValues" and "pattern" in arguments:
        if not isinstance(arguments["pattern"], str) or not arguments["pattern"]:
            raise ValueError(f"{label}: arguments.pattern must be a non-empty string")
    if metric == "missingValues" and "missingValues" in arguments:
        values = arguments["missingValues"]
        if not isinstance(values, list) or any(not _valid_scalar(value) for value in values):
            raise ValueError(f"{label}: arguments.missingValues must be a list of scalar values")


def _native_quality(  # noqa: PLR0912, PLR0915
    raw: Any, *, location: str, model_columns: set[str]
) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{location} config.meta.datacontract_cli.quality must be a list")
    rules = []
    supported_metrics = {
        "nullValues",
        "missingValues",
        "invalidValues",
        "duplicateValues",
        "rowCount",
    }
    for index, item in enumerate(raw):
        label = f"{location} quality[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{label} must be an object")
        unknown = set(item) - _QUALITY_KEYS
        if unknown:
            raise ValueError(f"{label} has unsupported fields: {', '.join(sorted(unknown))}")
        if item.get("type") != "library":
            raise ValueError(f"{label} must use type: library")
        metric = item.get("metric")
        if metric not in supported_metrics:
            raise ValueError(f"{label} has unsupported library metric {metric!r}")
        operators = set(item) & _QUALITY_OPERATORS
        if len(operators) != 1:
            raise ValueError(f"{label} must have exactly one comparison operator")
        operator = next(iter(operators))
        threshold = item[operator]
        if operator in {"mustBeBetween", "mustNotBeBetween"}:
            if (
                not isinstance(threshold, list)
                or len(threshold) != _RANGE_BOUND_COUNT
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    for value in threshold
                )
                or threshold[0] > threshold[1]
            ):
                raise ValueError(f"{label}: {operator} must be two finite, increasing numbers")
        elif (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or not math.isfinite(threshold)
        ):
            raise ValueError(f"{label}: {operator} must be a finite number")
        arguments = item.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise ValueError(f"{label} arguments must be an object")
        allowed_arguments = {
            "rowCount": set(),
            "duplicateValues": {"properties"},
            "nullValues": set(),
            "missingValues": {"missingValues"},
            "invalidValues": {"validValues", "pattern"},
        }[metric]
        unsupported_arguments = set(arguments) - allowed_arguments
        if unsupported_arguments:
            raise ValueError(
                f"{label} has unsupported {metric} arguments: "
                f"{', '.join(sorted(unsupported_arguments))}"
            )
        _validate_quality_arguments(label, metric, arguments, model_columns=model_columns)
        if metric == "rowCount" and location.startswith("column "):
            raise ValueError(f"{label}: rowCount is only valid at model level")
        if item.get("severity") not in (None, "error", "warning", "info"):
            raise ValueError(f"{label} has unsupported severity {item.get('severity')!r}")
        if item.get("unit") not in (None, "rows", "percent"):
            raise ValueError(f"{label} has unsupported unit {item.get('unit')!r}")
        if item.get("unit") == "percent" and metric not in {
            "nullValues",
            "missingValues",
            "invalidValues",
        }:
            raise ValueError(
                f"{label}: percent is only supported for "
                "nullValues, missingValues, and invalidValues"
            )
        if metric in {"nullValues", "missingValues", "invalidValues"} and location == "model":
            raise ValueError(f"{label}: {metric} is only supported at column level")
        if metric == "invalidValues" and not ({"validValues", "pattern"} & set(arguments)):
            raise ValueError(f"{label}: invalidValues requires validValues or pattern")
        if metric == "duplicateValues" and location == "model" and not arguments.get("properties"):
            raise ValueError(f"{label}: model duplicateValues requires arguments.properties")
        if metric == "duplicateValues" and location.startswith("column ") and arguments:
            raise ValueError(f"{label}: column duplicateValues does not accept arguments")
        rule = copy.deepcopy(item)
        if operator == "mustBeBetween" and threshold[0] == threshold[1]:
            rule.pop("mustBeBetween")
            rule["mustBe"] = threshold[0]
        rule.setdefault("arguments", None)
        if rule["arguments"] is None:
            rule.pop("arguments")
        rules.append(rule)
    return rules


def _map_test(  # noqa: PLR0912, PLR0915
    test: dict[str, Any],
    model: dict[str, Any],
    manifest: dict[str, Any],
    quality_by_column: dict[str, list[dict[str, Any]]],
    model_quality: list[dict[str, Any]],
) -> None:
    package, name, kwargs = _metadata(test)
    _check_test_config(test, kwargs)
    columns = model.get("columns") or {}
    severity = _severity(test)

    if package in {"dbt", "dbt-core"} and name in {"not_null", "unique", "accepted_values"}:
        column = kwargs.get("column_name") or test.get("column_name")
        if not isinstance(column, str) or column not in columns:
            raise _fail(test, f"column {column!r} is missing from model {model.get('name')}")
        allowed = {
            "not_null": {"column_name"},
            "unique": {"column_name"},
            "accepted_values": {"column_name", "values", "quote"},
        }[name]
        extra = set(kwargs) - allowed
        if extra:
            raise _fail(test, f"unsupported options: {', '.join(sorted(extra))}")
        if name == "not_null":
            rule = _quality_rule("nullValues", severity=severity, name=test.get("name"))
        elif name == "unique":
            rule = _quality_rule("duplicateValues", severity=severity, name=test.get("name"))
        else:
            values = kwargs.get("values")
            if (
                not isinstance(values, list)
                or not values
                or any(value is None or not _valid_scalar(value) for value in values)
            ):
                raise _fail(
                    test, "accepted_values requires a non-empty list of non-null scalar values"
                )
            if kwargs.get("quote", True) is not True:
                raise _fail(test, "accepted_values quote: false is not supported")
            rule = _quality_rule(
                "invalidValues",
                arguments={"validValues": [str(value) for value in values]},
                severity=severity,
                name=test.get("name"),
            )
        quality_by_column.setdefault(column, []).append(rule)
        return

    if package == "dbt_utils" and name == "unique_combination_of_columns":
        allowed = {"combination_of_columns"}
        extra = set(kwargs) - allowed
        if extra:
            raise _fail(test, f"unsupported options: {', '.join(sorted(extra))}")
        columns_in_key = kwargs.get("combination_of_columns")
        if not isinstance(columns_in_key, list) or not columns_in_key:
            raise _fail(test, "combination_of_columns must be a non-empty list")
        if any(column not in columns for column in columns_in_key):
            raise _fail(
                test, "combination_of_columns contains a column absent from the selected model"
            )
        definitely_not_null = {
            column
            for column, metadata in columns.items()
            if any(
                constraint.get("type") == "not_null"
                for constraint in metadata.get("constraints") or []
            )
        }
        for other_test in _attached_tests(manifest, model):
            other_package, other_name, other_kwargs = _metadata(other_test)
            other_column = other_kwargs.get("column_name") or other_test.get("column_name")
            other_config = other_test.get("config") or {}
            if (
                other_package == "dbt"
                and other_name == "not_null"
                and other_column in columns_in_key
                and str(other_config.get("severity", "error")).lower() == "error"
                and other_config.get("where") is None
            ):
                definitely_not_null.add(other_column)
        # dbt_utils groups NULLs into a key; require explicit non-null guarantees.
        if not set(columns_in_key).issubset(definitely_not_null):
            raise _fail(
                test,
                "composite uniqueness groups NULL keys; each key column must have "
                "an explicit not_null constraint or error-level test",
            )
        model_quality.append(
            _quality_rule(
                "duplicateValues",
                arguments={"properties": copy.deepcopy(columns_in_key)},
                severity=severity,
                name=test.get("name"),
            )
        )
        return

    if package == "dbt_expectations" and name in {
        "expect_table_row_count_to_equal",
        "expect_table_row_count_to_be_between",
    }:
        if name == "expect_table_row_count_to_equal":
            allowed = {"value"}
            if (
                set(kwargs) != allowed
                or isinstance(kwargs.get("value"), bool)
                or not isinstance(kwargs.get("value"), (int, float))
            ):
                raise _fail(test, "requires exactly one numeric value option")
            operator, value = "mustBe", kwargs["value"]
        else:
            allowed = {"min_value", "max_value"}
            if set(kwargs) - allowed or not {"min_value", "max_value"}.issubset(kwargs):
                raise _fail(test, "requires exactly min_value and max_value options")
            minimum, maximum = kwargs["min_value"], kwargs["max_value"]
            if any(
                isinstance(v, bool) or not isinstance(v, (int, float)) for v in (minimum, maximum)
            ):
                raise _fail(test, "min_value and max_value must be numeric")
            if minimum > maximum:
                raise _fail(test, "min_value cannot exceed max_value")
            if minimum == maximum:
                operator, value = "mustBe", minimum
            else:
                operator, value = "mustBeBetween", [minimum, maximum]
        model_quality.append(
            _quality_rule(
                "rowCount", operator=operator, value=value, severity=severity, name=test.get("name")
            )
        )
        return

    raise _fail(test, f"unsupported test package/name {package}.{name}")


def import_contract_from_manifest(manifest_path: str | Path, model_name: str) -> dict[str, Any]:
    """Import a selected dbt model plus supported tests into an ODCS dictionary."""
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    nodes = manifest.get("nodes") or {}
    models = [
        node
        for node in nodes.values()
        if node.get("resource_type") == "model" and node.get("name") == model_name
    ]
    if not models:
        raise ValueError(f"Model {model_name!r} not found in dbt manifest {manifest_path}")
    if len(models) > 1:
        raise ValueError(f"Model name {model_name!r} is ambiguous; use a unique dbt model name")
    model = models[0]

    # Let the maintained importer extract dbt types and explicit constraints,
    # but exclude test nodes so its test-derived PK inference cannot strengthen
    # the contract (for example, turning unique into a primary key).
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
    contract = imported.model_dump(by_alias=True, exclude_none=True)
    schema = contract["schema"][0]

    model_meta = (model.get("config") or {}).get("meta") or model.get("meta") or {}
    meta = model_meta.get("datacontract_cli") or {}
    model_column_names = set((model.get("columns") or {}).keys())
    model_quality = _native_quality(
        meta.get("quality"), location="model", model_columns=model_column_names
    )
    quality_by_column: dict[str, list[dict[str, Any]]] = {}
    for column_name, column in (model.get("columns") or {}).items():
        column_config_meta = (column.get("config") or {}).get("meta") or column.get("meta") or {}
        column_meta = column_config_meta.get("datacontract_cli") or {}
        quality_by_column[column_name] = _native_quality(
            column_meta.get("quality"),
            location=f"column {column_name}",
            model_columns=model_column_names,
        )

    for test in _attached_tests(manifest, model):
        _map_test(test, model, manifest, quality_by_column, model_quality)

    schema["quality"] = _native_quality(
        model_quality, location="model", model_columns=model_column_names
    )
    properties = {prop["name"]: prop for prop in schema.get("properties", [])}
    for column_name, quality in quality_by_column.items():
        if column_name not in properties:
            if quality:
                raise ValueError(
                    f"Quality rule references column {column_name!r} missing from imported schema"
                )
            continue
        if quality:
            properties[column_name]["quality"] = _native_quality(
                quality, location=f"column {column_name}", model_columns=model_column_names
            )
    return OpenDataContractStandard.model_validate(contract).model_dump(
        by_alias=True, exclude_none=True
    )


class DbtQualityManifestImporter(Importer):
    """Importer registered under ``dbt-quality`` without replacing the built-in ``dbt`` format."""

    def import_source(self, source: str, import_args: dict[str, Any]) -> OpenDataContractStandard:
        model_name = import_args.get("model") or import_args.get("dbt_model")
        if isinstance(model_name, list) and len(model_name) == 1:
            model_name = model_name[0]
        if not isinstance(model_name, str) or not model_name:
            raise ValueError("dbt-quality import requires exactly one model")
        contract = import_contract_from_manifest(source, model_name)
        return OpenDataContractStandard.model_validate(contract)


importer_factory.register_lazy_importer(
    name="dbt-quality",
    module_path="pipelines.common.capture.data_contract.dbt_importer",
    class_name="DbtQualityManifestImporter",
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    contract = OpenDataContractStandard.model_validate(
        import_contract_from_manifest(args.manifest, args.model)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        yaml.safe_dump(
            contract.model_dump(by_alias=True, exclude_none=True),
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
