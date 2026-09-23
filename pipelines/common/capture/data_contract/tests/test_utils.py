# -*- coding: utf-8 -*-
import unittest

from pipelines.common.capture.data_contract.utils import (
    adapt_contract_for_raw,
    add_local_server,
)


class ContractUtilsTest(unittest.TestCase):
    def test_adds_runtime_raw_filepath(self):
        contract = {
            "id": "staging_feed_info",
            "schema": [{"name": "staging_feed_info", "properties": []}],
        }

        result = add_local_server(
            contract,
            raw_filepath="/tmp/gtfs/raw/feed_info.txt",
        )

        assert result["servers"] == [
            {
                "server": "incoming",
                "type": "local",
                "path": "/tmp/gtfs/raw/feed_info.txt",
                "format": "csv",
            }
        ]

    def test_replaces_existing_server_without_duplicates(self):
        contract = {
            "servers": [
                {
                    "server": "incoming",
                    "type": "local",
                    "path": "/tmp/old.txt",
                    "format": "csv",
                }
            ]
        }

        result = add_local_server(contract, "/tmp/new.txt")

        assert len(result["servers"]) == 1
        assert result["servers"][0]["path"] == "/tmp/new.txt"

    def test_ignores_capture_metadata_columns(self):
        contract = {
            "schema": [
                {
                    "properties": [
                        {"name": "id"},
                        {"name": "timestamp_captura"},
                        {"name": "data_versao"},
                    ]
                }
            ]
        }

        result = adapt_contract_for_raw(
            contract,
            raw_filepath="/tmp/feed_info.txt",
            ignored_columns=("data_versao",),
        )

        assert result["schema"][0]["properties"] == [{"name": "id"}]
        assert result["servers"][0]["path"] == "/tmp/feed_info.txt"
        assert contract["schema"][0]["properties"][1]["name"] == "timestamp_captura"

    def test_replaces_importer_primary_key_with_source_primary_key(self):
        contract = {
            "schema": [
                {
                    "name": "feed_info",
                    "properties": [
                        {"name": "feed_publisher_name"},
                        {"name": "data_versao", "unique": True, "primaryKey": True},
                    ],
                }
            ]
        }

        result = adapt_contract_for_raw(
            contract,
            raw_filepath="/tmp/feed_info.txt",
            primary_keys=("feed_publisher_name",),
        )

        assert result["schema"][0]["properties"] == [
            {
                "name": "feed_publisher_name",
                "primaryKey": True,
                "primaryKeyPosition": 1,
            },
            {"name": "data_versao", "unique": True},
        ]

    def test_fails_when_source_primary_key_is_not_in_contract(self):
        contract = {"schema": [{"name": "feed_info", "properties": [{"name": "id"}]}]}

        try:
            adapt_contract_for_raw(contract, "/tmp/feed_info.txt", primary_keys=("missing",))
        except ValueError:
            pass
        else:
            raise AssertionError("Expected an error for a missing source primary key")


if __name__ == "__main__":
    unittest.main()
