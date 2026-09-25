# -*- coding: utf-8 -*-
# ruff: noqa: PT009, PT027 -- unittest matches the existing module tests.
import hashlib
import os
import unittest
from unittest.mock import Mock, patch

import requests
import yaml

from pipelines.common.capture.data_contract.repository import (
    contract_relative_path,
    download_contract_snapshot,
)

MODULE = "pipelines.common.capture.data_contract.repository"
SHA = "a" * 40
PATH = "contracts/source/base_sample.odcs.yaml"


def response(content=None, data=None):
    result = Mock()
    result.links = {}
    result.content = content
    result.json.return_value = data
    return result


class RepositoryTest(unittest.TestCase):
    def setUp(self):
        self.patch_env = patch.dict(os.environ, {}, clear=True)
        self.patch_env.start()
        self.addCleanup(self.patch_env.stop)
        self.patch_session = patch(f"{MODULE}.requests.Session")
        self.session = self.patch_session.start().return_value.__enter__.return_value
        self.addCleanup(self.patch_session.stop)
        self.contract = {"kind": "DataContract", "schema": [{"name": "base_sample"}]}
        self.content = yaml.safe_dump(self.contract).encode()

    def test_resolves_one_sha_and_deduplicates_downloads(self):
        other = "contracts/source/base_other.odcs.yaml"
        self.session.get.side_effect = [
            response(data={"sha": SHA}),
            response(self.content),
            response(self.content),
        ]
        result = download_contract_snapshot([PATH, other, PATH], "prod")
        self.assertEqual(result["sha"], SHA)
        self.assertEqual(result["ref"], "master")
        self.assertEqual(
            result["contracts"][PATH]["sha256"], hashlib.sha256(self.content).hexdigest()
        )
        calls = self.session.get.call_args_list
        self.assertEqual(len(calls), 1 + len(result["contracts"]))
        for call in calls[1:]:
            self.assertEqual(call.kwargs["params"], {"ref": SHA})

    def test_dev_follows_remote_head_of_local_branch(self):
        with patch(
            f"{MODULE}.subprocess.run", return_value=Mock(returncode=0, stdout="staging/new\n")
        ):
            for sha in (SHA, "b" * 40):
                self.session.get.side_effect = [response(data={"sha": sha}), response(self.content)]
                result = download_contract_snapshot([PATH], "dev")
                self.assertEqual(result["ref"], "staging/new")
                self.assertEqual(result["sha"], sha)
                self.assertEqual(self.session.get.call_args.kwargs["params"], {"ref": sha})
        self.assertTrue(self.session.get.call_args_list[0].args[0].endswith("staging%2Fnew"))

    def test_missing_checkout_or_detached_head_fails(self):
        with patch(f"{MODULE}.subprocess.run", return_value=Mock(returncode=1, stdout="")):
            with self.assertRaisesRegex(ValueError, "branch"):
                download_contract_snapshot([PATH], "dev")
        self.session.get.assert_not_called()

    def test_deployment_branch_is_used_without_git_and_follows_latest_remote_sha(self):
        with (
            patch.dict(os.environ, {"GIT_BRANCH": "staging/deployed"}),
            patch(f"{MODULE}.subprocess.run") as git,
        ):
            for sha in (SHA, "b" * 40):
                self.session.get.side_effect = [response(data={"sha": sha}), response(self.content)]
                result = download_contract_snapshot([PATH], "dev")
                self.assertEqual(result["ref"], "staging/deployed")
                self.assertEqual(result["sha"], sha)
        git.assert_not_called()

    def test_deleted_branch_fails_without_fallback(self):
        missing = response()
        missing.raise_for_status.side_effect = requests.HTTPError("404 branch deleted")
        self.session.get.return_value = missing
        with (
            patch.dict(os.environ, {"GIT_BRANCH": "staging/deleted"}),
            patch(f"{MODULE}.subprocess.run") as git,
        ):
            with self.assertRaises(requests.HTTPError):
                download_contract_snapshot([PATH], "dev")
        self.session.get.assert_called_once()
        git.assert_not_called()

    def test_prod_never_uses_local_branch(self):
        self.session.get.side_effect = [response(data={"sha": SHA}), response(self.content)]
        with (
            patch.dict(os.environ, {"GIT_BRANCH": "staging/ignored"}),
            patch(f"{MODULE}.subprocess.run") as git,
        ):
            download_contract_snapshot([PATH], "prod")
        git.assert_not_called()
        self.assertTrue(self.session.get.call_args_list[0].args[0].endswith("/commits/master"))

    def test_disabled_contracts_do_not_download(self):
        self.assertEqual(download_contract_snapshot([], "dev"), {})
        self.session.get.assert_not_called()

    def test_missing_contract_propagates_http_failure(self):
        missing = response()
        missing.raise_for_status.side_effect = requests.HTTPError("404")
        self.session.get.side_effect = [response(data={"sha": SHA}), missing]
        with self.assertRaises(requests.HTTPError):
            download_contract_snapshot([PATH], "prod")

    def test_invalid_sha_cannot_be_used_as_a_mutable_ref(self):
        self.session.get.return_value = response(data={"sha": "master"})
        with self.assertRaisesRegex(ValueError, "SHA"):
            download_contract_snapshot([PATH], "prod")
        self.session.get.assert_called_once()

    def test_invalid_yaml_contract_or_runtime_server_fails(self):
        for content in [
            b"[]",
            b"kind: DataContract\nschema: []",
            yaml.safe_dump(
                {**self.contract, "servers": [{"server": "incoming", "path": "/tmp/stale"}]}
            ).encode(),
        ]:
            with self.subTest(content=content):
                self.session.get.side_effect = [response(data={"sha": SHA}), response(content)]
                with self.assertRaises(ValueError):
                    download_contract_snapshot([PATH], "prod")

    def test_contract_path_rejects_traversal(self):
        with self.assertRaises(ValueError):
            contract_relative_path("../source", "base_sample")
        self.assertEqual(contract_relative_path("source", "base_sample"), PATH)


if __name__ == "__main__":
    unittest.main()
