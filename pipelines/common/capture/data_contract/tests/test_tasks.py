# -*- coding: utf-8 -*-
# ruff: noqa: PT027 -- usa unittest, como os testes existentes deste módulo.
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from pipelines.common.capture.data_contract.tasks import validate_raw_data_contract

UPDATED_ROW_COUNT = 2
TASKS = "pipelines.common.capture.data_contract.tasks"


class DataContractTaskTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.manifest = root / "manifest.json"
        self.manifest.write_text("{}", encoding="utf-8")
        self.contract = root / "contract.yaml"
        self.raw = root / "feed_info.txt"
        self.raw.write_text("feed_publisher_name\nSMTR\n", encoding="utf-8")
        self.context = SimpleNamespace(
            captured_raw_filepaths=[str(self.raw)],
            source=SimpleNamespace(
                table_id="feed_info",
                primary_keys=["feed_publisher_name"],
                raw_filetype="csv",
            ),
        )
        self.imported = {
            "schema": [
                {
                    "name": "base_feed_info",
                    "quality": [{"type": "library", "metric": "rowCount", "mustBe": 1}],
                    "properties": [
                        {"name": "feed_publisher_name"},
                        {"name": "timestamp_captura"},
                        {"name": "data_versao"},
                    ],
                }
            ],
        }

    def generate(self, **kwargs):
        Path(kwargs["contract_path"]).write_text(yaml.safe_dump(self.imported), encoding="utf-8")

    def validate(self):
        validate_raw_data_contract.fn(
            context=self.context,
            manifest_path=self.manifest,
            contract_path=self.contract,
            ignored_columns=("data_versao",),
        )

    def test_regenerates_existing_contract_and_tests_local_copy(self):
        self.contract.write_text("schema: []\n", encoding="utf-8")
        runtime_contracts = []

        def execute(command):
            assert command[:4] == [
                "contract-python",
                "-c",
                "from datacontract.cli import main; main()",
                "test",
            ]
            assert command[-2:] == ["--server", "incoming"]
            runtime_contracts.append(yaml.safe_load(Path(command[4]).read_text()))

        with (
            patch(f"{TASKS}.get_datacontract_python", return_value="contract-python"),
            patch(f"{TASKS}.generate_contract", side_effect=self.generate) as generate,
            patch(f"{TASKS}.run_datacontract", side_effect=execute),
        ):
            self.validate()
            self.imported["schema"][0]["quality"][0]["mustBe"] = UPDATED_ROW_COUNT
            self.validate()

        # Both executions must use the current manifest.
        assert generate.call_count == len(runtime_contracts)
        assert runtime_contracts[0]["schema"][0]["quality"][0]["mustBe"] == 1
        assert runtime_contracts[1]["schema"][0]["quality"][0]["mustBe"] == UPDATED_ROW_COUNT
        assert runtime_contracts[0]["servers"][0]["path"] == str(self.raw)
        persisted = yaml.safe_load(self.contract.read_text())
        assert "servers" not in persisted
        assert persisted["schema"][0]["properties"] == [
            {
                "name": "feed_publisher_name",
                "primaryKey": True,
                "primaryKeyPosition": 1,
            }
        ]

    def test_import_failure_preserves_previous_artifact_and_does_not_test(self):
        self.contract.write_text("previous artifact\n", encoding="utf-8")
        with (
            patch(f"{TASKS}.get_datacontract_python", return_value="contract-python"),
            patch(f"{TASKS}.generate_contract", side_effect=RuntimeError("unsupported test")),
            patch(f"{TASKS}.run_datacontract") as execute,
            self.assertRaisesRegex(RuntimeError, "unsupported test"),
        ):
            self.validate()
        execute.assert_not_called()
        assert self.contract.read_text() == "previous artifact\n"

    def test_validation_failure_propagates_and_stops_remaining_files(self):
        self.context.captured_raw_filepaths.append("another.csv")
        with (
            patch(f"{TASKS}.get_datacontract_python", return_value="contract-python"),
            patch(f"{TASKS}.generate_contract", side_effect=self.generate),
            patch(
                f"{TASKS}.run_datacontract", side_effect=RuntimeError("row count failed")
            ) as execute,
            self.assertRaisesRegex(RuntimeError, "row count failed"),
        ):
            self.validate()
        execute.assert_called_once()

    def test_no_files_fails_before_generation(self):
        self.context.captured_raw_filepaths = []
        with (
            patch(f"{TASKS}.generate_contract") as generate,
            self.assertRaisesRegex(ValueError, "Nenhum arquivo bruto"),
        ):
            self.validate()
        generate.assert_not_called()


if __name__ == "__main__":
    unittest.main()
