# -*- coding: utf-8 -*-
# ruff: noqa: PT009 -- unittest matches the existing module tests.
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pipelines.capture__riorotativo_credenciados.flow import capture__riorotativo_credenciados
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks

MODULE = "pipelines.common.capture.default_capture.flow"


class CaptureContractGateTest(unittest.TestCase):
    def run_capture(self, *, enabled, failure=None, skip=False, preparation_failure=None):
        contexts = [SimpleNamespace(source=SimpleNamespace(validate_data_contract=enabled))]
        names = (
            "get_run_env",
            "setup_environment",
            "initialize_sentry",
            "get_scheduled_timestamp",
            "create_capture_contexts",
            "get_raw_data",
            "prepare_data_contracts",
            "validate_raw_data_contract",
            "upload_raw_file_to_gcs",
            "transform_raw_to_nested_structure",
            "upload_source_data_to_gcs",
        )
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    f"{MODULE}.runtime",
                    SimpleNamespace(deployment=SimpleNamespace(name="capture--prod")),
                )
            )
            mocks = {name: stack.enter_context(patch(f"{MODULE}.{name}")) for name in names}
            mocks["get_run_env"].return_value = "prod"
            mocks["create_capture_contexts"].return_value = contexts
            mocks["prepare_data_contracts"].return_value = {"sha": "a" * 40}
            mocks["prepare_data_contracts"].side_effect = preparation_failure
            if failure:
                mocks["validate_raw_data_contract"].map.return_value.result.side_effect = failure
            caught = None
            result = None
            try:
                result = create_capture_flows_default_tasks(
                    env="prod",
                    sources=[],
                    timestamp=None,
                    create_extractor_task=Mock(),
                    recapture=False,
                    recapture_days=0,
                    recapture_timestamps=[],
                    skip_data_contract_validation=skip,
                )
            except RuntimeError as exc:
                caught = exc
            return mocks, result, caught

    def test_flow_exposes_skip_with_safe_default_and_forwards_it(self):
        parameter = capture__riorotativo_credenciados.parameters.properties[
            "skip_data_contract_validation"
        ]
        self.assertEqual(parameter["type"], "boolean")
        self.assertFalse(parameter["default"])
        with patch(
            "pipelines.capture__riorotativo_credenciados.flow.create_capture_flows_default_tasks"
        ) as capture:
            capture__riorotativo_credenciados.fn(skip_data_contract_validation=True)
        self.assertTrue(capture.call_args.kwargs["skip_data_contract_validation"])

    def test_invalid_raw_blocks_both_uploads(self):
        mocks, _, error = self.run_capture(enabled=True, failure=RuntimeError("invalid raw"))
        self.assertEqual(str(error), "invalid raw")
        mocks["prepare_data_contracts"].assert_called_once()
        mocks["upload_raw_file_to_gcs"].map.assert_not_called()
        mocks["upload_source_data_to_gcs"].map.assert_not_called()

    def test_disabled_sources_keep_existing_capture_path(self):
        mocks, _, error = self.run_capture(enabled=False)
        self.assertIsNone(error)
        mocks["prepare_data_contracts"].assert_not_called()
        mocks["validate_raw_data_contract"].map.assert_not_called()
        mocks["upload_raw_file_to_gcs"].map.assert_called_once()
        mocks["upload_source_data_to_gcs"].map.assert_called_once()

    def test_skip_avoids_contract_download_and_validation_but_uploads(self):
        mocks, _, error = self.run_capture(
            enabled=True, skip=True, preparation_failure=RuntimeError("branch deleted")
        )
        self.assertIsNone(error)
        mocks["get_raw_data"].map.assert_called_once()
        mocks["prepare_data_contracts"].assert_not_called()
        mocks["validate_raw_data_contract"].map.assert_not_called()
        mocks["upload_raw_file_to_gcs"].map.assert_called_once()
        mocks["upload_source_data_to_gcs"].map.assert_called_once()

    def test_default_does_not_skip_missing_branch_failure(self):
        mocks, _, error = self.run_capture(
            enabled=True, preparation_failure=RuntimeError("branch deleted")
        )
        self.assertEqual(str(error), "branch deleted")
        mocks["upload_raw_file_to_gcs"].map.assert_not_called()
        mocks["upload_source_data_to_gcs"].map.assert_not_called()

    def test_validated_capture_prepares_once_and_uploads(self):
        mocks, result, error = self.run_capture(enabled=True)
        self.assertIsNone(error)
        mocks["prepare_data_contracts"].assert_called_once()
        mocks["validate_raw_data_contract"].map.assert_called_once()
        mocks["upload_raw_file_to_gcs"].map.assert_called_once()
        self.assertEqual(result["data_contract_snapshot"]["sha"], "a" * 40)


if __name__ == "__main__":
    unittest.main()
