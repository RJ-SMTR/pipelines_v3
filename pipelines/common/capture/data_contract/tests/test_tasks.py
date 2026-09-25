# -*- coding: utf-8 -*-
# ruff: noqa: PT009, PT027 -- unittest matches the existing module tests.
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from pipelines.common.capture.data_contract.tasks import (
    prepare_data_contracts,
    validate_raw_data_contract,
)

TASKS = "pipelines.common.capture.data_contract.tasks"
SHA = "a" * 40
CONTRACT_PATH = "contracts/source/base_sample.odcs.yaml"


class DataContractTaskTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.raw = Path(self.directory.name) / "raw.csv"
        self.raw.write_text("id\n1\n", encoding="utf-8")
        self.context = SimpleNamespace(
            captured_raw_filepaths=[str(self.raw)],
            source=SimpleNamespace(
                source_name="source",
                table_id="sample",
                raw_filetype="csv",
                validate_data_contract=True,
                data_contract_model="base_sample",
            ),
        )
        self.snapshot = {
            "repository": "owner/repo",
            "ref": "master",
            "sha": SHA,
            "contracts": {
                CONTRACT_PATH: {
                    "sha256": "example-digest",
                    "contract": {
                        "schema": [{"name": "base_sample", "properties": [{"name": "id"}]}]
                    },
                }
            },
        }

    def test_downloads_only_enabled_source_paths(self):
        disabled = copy.deepcopy(self.context)
        disabled.source.validate_data_contract = False
        with patch(f"{TASKS}.download_contract_snapshot", return_value=self.snapshot) as download:
            result = prepare_data_contracts.fn([self.context, disabled], "prod")
        download.assert_called_once_with([CONTRACT_PATH], "prod")
        self.assertEqual(result, self.snapshot)

    def test_each_file_uses_runtime_copy_and_returns_provenance(self):
        self.context.captured_raw_filepaths.append(str(self.raw.with_name("another.csv")))
        original = copy.deepcopy(self.snapshot)
        tested = []

        def execute(command):
            self.assertEqual(
                command[:4],
                [sys.executable, "-c", "from datacontract.cli import main; main()", "test"],
            )
            self.assertEqual(command[-2:], ["--server", "incoming"])
            tested.append(yaml.safe_load(Path(command[4]).read_text()))

        with (
            patch(f"{TASKS}.run_datacontract", side_effect=execute),
        ):
            result = validate_raw_data_contract.fn(self.context, self.snapshot)
        self.assertEqual(
            [c["servers"][0]["path"] for c in tested], self.context.captured_raw_filepaths
        )
        self.assertEqual(self.snapshot, original)
        self.assertEqual(result["sha"], SHA)
        self.assertEqual(result["sha256"], "example-digest")
        self.assertEqual(result["status"], "passed")

    def test_validation_failure_propagates_and_stops_remaining_files(self):
        self.context.captured_raw_filepaths.append("another.csv")
        with (
            patch(f"{TASKS}.run_datacontract", side_effect=RuntimeError("invalid raw")) as execute,
            self.assertRaisesRegex(RuntimeError, "invalid raw"),
        ):
            validate_raw_data_contract.fn(self.context, self.snapshot)
        execute.assert_called_once()

    def test_disabled_source_needs_neither_snapshot_nor_cli(self):
        self.context.source.validate_data_contract = False
        with patch(f"{TASKS}.run_datacontract") as command:
            self.assertIsNone(validate_raw_data_contract.fn(self.context, {}))
        command.assert_not_called()

    def test_missing_contract_or_wrong_model_does_not_run_cli(self):
        with patch(f"{TASKS}.run_datacontract") as execute:
            with self.assertRaises(KeyError):
                validate_raw_data_contract.fn(self.context, {"contracts": {}})
            self.snapshot["contracts"][CONTRACT_PATH]["contract"]["schema"][0]["name"] = "wrong"
            with self.assertRaisesRegex(ValueError, "Schema inesperado"):
                validate_raw_data_contract.fn(self.context, self.snapshot)
        execute.assert_not_called()

    def test_no_files_fails(self):
        self.context.captured_raw_filepaths = []
        with self.assertRaisesRegex(ValueError, "Nenhum arquivo bruto"):
            validate_raw_data_contract.fn(self.context, self.snapshot)


if __name__ == "__main__":
    unittest.main()
