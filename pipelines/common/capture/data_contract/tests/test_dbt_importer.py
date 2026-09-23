# -*- coding: utf-8 -*-
# ruff: noqa: PT009, PT027 -- unittest matches the existing module test suite.
import copy
import json
import tempfile
import unittest
from pathlib import Path

from datacontract.data_contract import DataContract

from pipelines.common.capture.data_contract.dbt_importer import import_contract_from_manifest


def model_node(**overrides):
    node = {
        "resource_type": "model",
        "unique_id": "model.project.sample",
        "name": "sample",
        "description": "Sample model",
        "config": {"materialized": "table", "meta": {}},
        "columns": {
            "id": {
                "name": "id",
                "data_type": "INT64",
                "description": "Identifier",
                "constraints": [],
            },
            "status": {
                "name": "status",
                "data_type": "STRING",
                "description": "Status",
                "constraints": [],
            },
        },
        "constraints": [],
        "tags": [],
    }
    node.update(overrides)
    return node


def generic_test(  # noqa: PLR0913
    name, kwargs, *, package="dbt", severity="error", config=None, attached_node=None
):
    suffix = kwargs.get("column_name", "_".join(kwargs.get("combination_of_columns", [])))
    node = {
        "resource_type": "test",
        "unique_id": f"test.project.{package}_{name}_{suffix}",
        "name": f"{package}_{name}_sample",
        "test_metadata": {
            "name": name,
            "namespace": None if package == "dbt" else package,
            "kwargs": {"model": "{{ get_where_subquery(ref('sample')) }}", **kwargs},
        },
        "config": {"enabled": True, "severity": severity, **(config or {})},
        "depends_on": {"nodes": ["model.project.sample"]},
    }
    if attached_node:
        node["attached_node"] = attached_node
    return node


class DBTImporterTest(unittest.TestCase):
    def import_manifest(self, manifest):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            return import_contract_from_manifest(path, "sample")

    def base_manifest(self, tests=None, **model_overrides):
        model = model_node(**model_overrides)
        nodes = {model["unique_id"]: model}
        for test in tests or []:
            nodes[test["unique_id"]] = test
        return {
            "metadata": {"project_name": "Example Project", "adapter_type": "bigquery"},
            "nodes": nodes,
            "child_map": {model["unique_id"]: [t["unique_id"] for t in tests or []]},
        }

    def test_maps_core_column_tests_and_warning_severity_without_inferred_primary_key(self):
        manifest = self.base_manifest(
            [
                generic_test("not_null", {"column_name": "id"}),
                generic_test("unique", {"column_name": "id"}),
                generic_test(
                    "accepted_values",
                    {"column_name": "status", "values": ["open", "closed"]},
                    severity="warn",
                ),
            ]
        )
        contract = self.import_manifest(manifest)
        schema = contract["schema"][0]
        self.assertEqual(
            schema["properties"][0]["quality"],
            [
                {
                    "type": "library",
                    "metric": "nullValues",
                    "mustBe": 0,
                    "name": "dbt_not_null_sample",
                },
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "name": "dbt_unique_sample",
                },
            ],
        )
        self.assertEqual(
            schema["properties"][1]["quality"],
            [
                {
                    "type": "library",
                    "metric": "invalidValues",
                    "mustBe": 0,
                    "arguments": {"validValues": ["open", "closed"]},
                    "severity": "warning",
                    "name": "dbt_accepted_values_sample",
                }
            ],
        )
        self.assertNotIn("primaryKey", schema["properties"][0])
        self.assertEqual(schema["quality"], [])

    def test_maps_table_row_count_equal_between_and_disabled_tests(self):
        tests = [
            generic_test(
                "expect_table_row_count_to_equal", {"value": 1}, package="dbt_expectations"
            ),
            generic_test(
                "expect_table_row_count_to_be_between",
                {"min_value": 2, "max_value": 8},
                package="dbt_expectations",
            ),
            generic_test("unsupported_test", {}, config={"enabled": False}),
        ]
        schema = self.import_manifest(self.base_manifest(tests))["schema"][0]
        self.assertEqual(
            schema["quality"],
            [
                {
                    "type": "library",
                    "metric": "rowCount",
                    "mustBe": 1,
                    "name": "dbt_expectations_expect_table_row_count_to_equal_sample",
                },
                {
                    "type": "library",
                    "metric": "rowCount",
                    "mustBeBetween": [2, 8],
                    "name": "dbt_expectations_expect_table_row_count_to_be_between_sample",
                },
            ],
        )

    def test_maps_composite_unique_only_with_all_not_null_guarantees(self):
        model = model_node()
        model["columns"]["id"]["constraints"] = [{"type": "not_null"}]
        model["columns"]["status"]["constraints"] = [{"type": "not_null"}]
        test = generic_test(
            "unique_combination_of_columns",
            {"combination_of_columns": ["id", "status"]},
            package="dbt_utils",
        )
        schema = self.import_manifest(self.base_manifest([test], **model))["schema"][0]
        self.assertEqual(
            schema["quality"],
            [
                {
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "arguments": {"properties": ["id", "status"]},
                    "name": "dbt_utils_unique_combination_of_columns_sample",
                }
            ],
        )

    def test_maps_composite_unique_when_not_null_tests_are_attached(self):
        tests = [
            generic_test("not_null", {"column_name": "id"}),
            generic_test("not_null", {"column_name": "status"}),
            generic_test(
                "unique_combination_of_columns",
                {"combination_of_columns": ["id", "status"]},
                package="dbt_utils",
            ),
        ]
        self.assertEqual(
            len(self.import_manifest(self.base_manifest(tests))["schema"][0]["quality"]), 1
        )

    def test_rejects_composite_unique_without_null_guarantees_and_filtered_tests(self):
        composite = generic_test(
            "unique_combination_of_columns",
            {"combination_of_columns": ["id", "status"]},
            package="dbt_utils",
        )
        with self.assertRaisesRegex(
            ValueError, "each key column must have an explicit not_null constraint"
        ):
            self.import_manifest(self.base_manifest([composite]))
        filtered = generic_test("not_null", {"column_name": "id"}, config={"where": "id > 0"})
        with self.assertRaisesRegex(ValueError, "config.where"):
            self.import_manifest(self.base_manifest([filtered]))

    def test_rejects_unknown_namespace_and_singular_sql_tests(self):
        unknown = generic_test(
            "accepted_values", {"column_name": "status", "values": ["open"]}, package="my_pkg"
        )
        with self.assertRaisesRegex(
            ValueError, "unsupported test package/name my_pkg.accepted_values"
        ):
            self.import_manifest(self.base_manifest([unknown]))
        singular = {
            "resource_type": "test",
            "unique_id": "test.project.singular",
            "name": "singular",
            "config": {"enabled": True},
            "depends_on": {"nodes": ["model.project.sample"]},
        }
        with self.assertRaisesRegex(ValueError, "only generic tests with test_metadata"):
            self.import_manifest(self.base_manifest([singular]))

    def test_native_model_and_column_quality_rules_are_preserved_and_validated(self):
        model = model_node()
        model["config"]["meta"] = {
            "datacontract_cli": {
                "quality": [
                    {
                        "type": "library",
                        "metric": "rowCount",
                        "mustBeGreaterThan": 0,
                        "description": "not empty",
                    }
                ]
            }
        }
        model["columns"]["status"]["config"] = {
            "meta": {
                "datacontract_cli": {
                    "quality": [
                        {
                            "type": "library",
                            "metric": "invalidValues",
                            "mustBe": 0,
                            "arguments": {"validValues": ["ok"]},
                        }
                    ]
                }
            }
        }
        original = copy.deepcopy(model)
        schema = self.import_manifest(self.base_manifest(**model))["schema"][0]
        self.assertEqual(schema["quality"][0]["metric"], "rowCount")
        self.assertEqual(model, original)
        model["config"]["meta"]["datacontract_cli"]["quality"].append(
            {"type": "library", "metric": "rowCount", "mustBeBetween": [1, 1]}
        )
        equal_range_schema = self.import_manifest(self.base_manifest(**model))["schema"][0]
        self.assertEqual(equal_range_schema["quality"][1]["mustBe"], 1)
        self.assertNotIn("mustBeBetween", equal_range_schema["quality"][1])
        self.assertEqual(
            schema["properties"][1]["quality"][0]["arguments"], {"validValues": ["ok"]}
        )
        model["config"]["meta"]["datacontract_cli"]["quality"][0]["type"] = "custom"
        with self.assertRaisesRegex(ValueError, "must use type: library"):
            self.import_manifest(self.base_manifest(**model))

    def test_only_enabled_attached_tests_are_considered(self):
        test = generic_test("unsupported_test", {}, config={"enabled": False})
        contract = self.import_manifest(self.base_manifest([test]))
        self.assertEqual(contract["schema"][0]["quality"], [])

    def test_foreign_attached_node_is_not_imported_even_when_model_is_a_dependency(self):
        test = generic_test("unsupported", {}, attached_node="model.project.other")
        schema = self.import_manifest(self.base_manifest([test]))["schema"][0]
        assert schema["quality"] == []

    def test_native_rules_reject_invalid_thresholds_scopes_and_arguments(self):
        invalid_rules = [
            {"metric": "rowCount", "mustBe": None},
            {"metric": "rowCount", "mustBe": float("nan")},
            {"metric": "rowCount", "mustBeBetween": [1]},
            {"metric": "rowCount", "mustBeBetween": [4, 2]},
            {"metric": "rowCount", "mustBe": 1, "mustBeGreaterThan": 0},
            {"metric": "rowCount", "mustBe": 1, "unit": "percent"},
            {"metric": "nullValues", "mustBe": 0},
            {"metric": "unknown", "mustBe": 0},
            {"metric": "duplicateValues", "mustBe": 0, "arguments": {"properties": ["absent"]}},
        ]
        for rule in invalid_rules:
            with self.subTest(rule=rule):
                manifest = self.base_manifest(
                    config={
                        "meta": {"datacontract_cli": {"quality": [{"type": "library", **rule}]}}
                    }
                )
                with self.assertRaises(ValueError):
                    self.import_manifest(manifest)

    def test_native_missing_values_allows_null_and_equal_range_is_preserved(self):
        model = model_node()
        model["config"]["meta"] = {
            "datacontract_cli": {
                "quality": [{"type": "library", "metric": "rowCount", "mustBeBetween": [1, 1]}]
            }
        }
        model["columns"]["status"]["config"] = {
            "meta": {
                "datacontract_cli": {
                    "quality": [
                        {
                            "type": "library",
                            "metric": "missingValues",
                            "mustBe": 0,
                            "arguments": {"missingValues": [None, "", "N/A"]},
                        }
                    ]
                }
            }
        }
        schema = self.import_manifest(self.base_manifest(**model))["schema"][0]
        assert schema["quality"][0]["mustBe"] == 1
        assert schema["properties"][1]["quality"][0]["arguments"]["missingValues"] == [
            None,
            "",
            "N/A",
        ]

    def test_generated_row_count_rejects_nonfinite_value(self):
        test = generic_test(
            "expect_table_row_count_to_equal", {"value": float("inf")}, package="dbt_expectations"
        )
        with self.assertRaisesRegex(ValueError, "finite number"):
            self.import_manifest(self.base_manifest([test]))

    def test_warning_not_null_does_not_guarantee_composite_uniqueness(self):
        tests = [
            generic_test(
                "unique_combination_of_columns",
                {"combination_of_columns": ["id"]},
                package="dbt_utils",
            ),
            generic_test("not_null", {"column_name": "id"}, severity="warn"),
        ]
        with self.assertRaisesRegex(ValueError, "each key column"):
            self.import_manifest(self.base_manifest(tests))

    def test_registered_importer_accepts_singleton_list_and_rejects_multiple_models(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(json.dumps(self.base_manifest()), encoding="utf-8")
            contract = DataContract.import_from_source(
                format="dbt-quality", source=str(path), dbt_model=["sample"]
            )
            assert contract.schema_[0].name == "sample"
            with self.assertRaisesRegex(ValueError, "exactly one model"):
                DataContract.import_from_source(
                    format="dbt-quality", source=str(path), dbt_model=["sample", "other"]
                )

    def test_registered_importer_uses_a_distinct_format(self):
        manifest = self.base_manifest([generic_test("not_null", {"column_name": "id"})])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            contract = DataContract.import_from_source(
                format="dbt-quality", source=str(path), dbt_model="sample"
            )
        self.assertEqual(contract.schema_[0].properties[0].quality[0].metric, "nullValues")


if __name__ == "__main__":
    unittest.main()
