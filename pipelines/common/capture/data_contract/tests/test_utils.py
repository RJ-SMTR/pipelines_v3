# -*- coding: utf-8 -*-
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from datacontract.data_contract import DataContract

from pipelines.common.capture.data_contract.utils import (
    ContractValidationResult,
    persist_contract_validation_result,
    validate_contract_files,
)

PIPELINES_DIR = Path(__file__).resolve().parents[4]
CONTRACTS = PIPELINES_DIR / "capture__riorotativo_vagas" / "contracts"
FIXTURES = Path(__file__).parent / "fixtures"


class ValidateContractFilesTest(unittest.TestCase):
    def test_all_contracts_are_valid_odcs(self):
        contract_paths = [
            PIPELINES_DIR
            / "capture__riorotativo_credenciados"
            / "contracts"
            / "entidade_05019730000158.odcs.yaml",
            PIPELINES_DIR
            / "capture__riorotativo_credenciados"
            / "contracts"
            / "entidade_34152025000122.odcs.yaml",
            CONTRACTS / "area_estacionamento.odcs.yaml",
            CONTRACTS / "perfil_funcionamento.odcs.yaml",
        ]

        for contract_path in contract_paths:
            with self.subTest(contract_path=contract_path):
                assert DataContract(data_contract_file=str(contract_path)).lint().has_passed()

    def test_valid_fixture_passes(self):
        result = self._validate("area_estacionamento_valid.csv")

        assert not result.failed_contract
        assert result.result == "passed"
        assert result.violations == []

    def test_missing_column_fails(self):
        result = self._validate("area_estacionamento_missing_column.csv")

        assert result.failed_contract
        assert "area_poligono" in {violation["field"] for violation in result.violations}

    def test_renamed_column_fails(self):
        result = self._validate("area_estacionamento_renamed_column.csv")

        assert result.failed_contract
        assert "area_nome" in {violation["field"] for violation in result.violations}

    def test_invalid_type_fails(self):
        result = self._validate("area_estacionamento_invalid_type.csv")

        assert result.failed_contract
        assert "model_quality_sql" in {violation["type"] for violation in result.violations}

    def test_null_required_field_fails(self):
        result = self._validate("area_estacionamento_null_required.csv")

        assert result.failed_contract
        assert "area_codigo" in {violation["field"] for violation in result.violations}

    def _validate(self, fixture_name: str):
        return validate_contract_files(
            contract_path=CONTRACTS / "area_estacionamento.odcs.yaml",
            raw_filepaths=[str(FIXTURES / fixture_name)],
        )


class PersistContractValidationResultTest(unittest.TestCase):
    def test_result_is_merged_by_table_and_capture_timestamp(self):
        client = MagicMock()
        client.project = "rj-smtr-dev"
        source = SimpleNamespace(
            dataset_id="source_riorotativo",
            table_id="area_estacionamento",
            client=MagicMock(return_value=client),
        )
        context = SimpleNamespace(
            source=source,
            timestamp=datetime(
                2026,
                7,
                16,
                8,
                tzinfo=ZoneInfo("America/Sao_Paulo"),
            ),
        )
        validation = ContractValidationResult(
            failed_contract=True,
            contract_id="riorotativo-area-estacionamento",
            contract_version="1.0.0",
            datacontract_cli_version="1.0.13",
            result="failed",
            violations=[{"field": "area_codigo"}],
            raw_filepaths=["/tmp/area_estacionamento.csv"],
        )

        persist_contract_validation_result(
            context=context,
            validation=validation,
            flow_run_id="flow-run-id",
        )

        expected_query_count = 2
        assert client.query.call_count == expected_query_count
        create_query = client.query.call_args_list[0].args[0]
        merge_query = client.query.call_args_list[1].args[0]
        assert "CREATE TABLE IF NOT EXISTS" in create_query
        assert "target.table_id = source.table_id" in merge_query
        assert "target.timestamp_captura = source.timestamp_captura" in merge_query

        job_config = client.query.call_args_list[1].kwargs["job_config"]
        parameters = {
            parameter.name: parameter.value
            for parameter in job_config.query_parameters
            if hasattr(parameter, "value")
        }
        assert parameters["table_id"] == "area_estacionamento"
        assert parameters["failed_contract"] is True
        assert parameters["contract_version"] == "1.0.0"
        assert parameters["flow_run_id"] == "flow-run-id"


if __name__ == "__main__":
    unittest.main()
