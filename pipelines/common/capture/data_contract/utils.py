# -*- coding: utf-8 -*-
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from datacontract.data_contract import DataContract
from datacontract.model.run import ResultEnum
from google.cloud import bigquery

from pipelines.common.capture.data_contract import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


@dataclass
class ContractValidationResult:
    failed_contract: bool
    contract_id: str
    contract_version: str
    datacontract_cli_version: str
    result: str
    violations: list[dict]
    raw_filepaths: list[str]


def validate_contract_files(
    contract_path: str | Path,
    raw_filepaths: list[str],
) -> ContractValidationResult:
    contract_path = Path(contract_path)
    violations = []
    results = []
    contract_id = ""
    contract_version = ""
    cli_version = ""

    for raw_filepath in raw_filepaths:
        data_contract = DataContract(data_contract_file=str(contract_path)).get_data_contract()
        incoming_server = next(
            server for server in data_contract.servers if server.server == "incoming"
        )
        incoming_server.path = raw_filepath

        run = DataContract(
            data_contract=data_contract,
            server="incoming",
            check_categories={"schema", "quality"},
        ).test()

        contract_id = run.dataContractId or data_contract.id
        contract_version = run.dataContractVersion or data_contract.version
        cli_version = run.datacontractCliVersion
        results.append(run.result)

        violations.extend(
            {
                "filepath": raw_filepath,
                "type": check.type,
                "name": check.name,
                "field": check.field,
                "result": check.result.value,
                "reason": check.reason,
            }
            for check in run.checks
            if check.result in {ResultEnum.failed, ResultEnum.error}
        )

    result = _aggregate_result(results)
    return ContractValidationResult(
        failed_contract=result in {ResultEnum.failed, ResultEnum.error},
        contract_id=contract_id,
        contract_version=contract_version,
        datacontract_cli_version=cli_version,
        result=result.value,
        violations=violations,
        raw_filepaths=raw_filepaths,
    )


def persist_contract_validation_result(
    context: SourceCaptureContext,
    validation: ContractValidationResult,
    flow_run_id: Optional[str],
) -> None:
    source = context.source
    client = source.client("bigquery")
    table_full_name = f"{client.project}.{source.dataset_id}.{constants.RESULT_TABLE_ID}"
    timestamp_captura = context.timestamp.replace(tzinfo=None)

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{table_full_name}` (
            data DATE,
            table_id STRING,
            timestamp_captura DATETIME,
            failed_contract BOOL,
            contract_id STRING,
            contract_version STRING,
            datacontract_cli_version STRING,
            validation_result STRING,
            violations JSON,
            raw_filepaths ARRAY<STRING>,
            flow_run_id STRING,
            datetime_validacao DATETIME
        )
        PARTITION BY data
        CLUSTER BY table_id
        """
    ).result()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data", "DATE", timestamp_captura.date()),
            bigquery.ScalarQueryParameter("table_id", "STRING", source.table_id),
            bigquery.ScalarQueryParameter("timestamp_captura", "DATETIME", timestamp_captura),
            bigquery.ScalarQueryParameter("failed_contract", "BOOL", validation.failed_contract),
            bigquery.ScalarQueryParameter("contract_id", "STRING", validation.contract_id),
            bigquery.ScalarQueryParameter(
                "contract_version", "STRING", validation.contract_version
            ),
            bigquery.ScalarQueryParameter(
                "datacontract_cli_version",
                "STRING",
                validation.datacontract_cli_version,
            ),
            bigquery.ScalarQueryParameter("validation_result", "STRING", validation.result),
            bigquery.ScalarQueryParameter(
                "violations", "STRING", json.dumps(validation.violations)
            ),
            bigquery.ArrayQueryParameter("raw_filepaths", "STRING", validation.raw_filepaths),
            bigquery.ScalarQueryParameter("flow_run_id", "STRING", flow_run_id),
        ]
    )

    client.query(
        f"""
        MERGE `{table_full_name}` AS target
        USING (
            SELECT
                @data AS data,
                @table_id AS table_id,
                @timestamp_captura AS timestamp_captura,
                @failed_contract AS failed_contract,
                @contract_id AS contract_id,
                @contract_version AS contract_version,
                @datacontract_cli_version AS datacontract_cli_version,
                @validation_result AS validation_result,
                PARSE_JSON(@violations) AS violations,
                @raw_filepaths AS raw_filepaths,
                @flow_run_id AS flow_run_id,
                CURRENT_DATETIME("America/Sao_Paulo") AS datetime_validacao
        ) AS source
        ON
            target.table_id = source.table_id
            AND target.timestamp_captura = source.timestamp_captura
        WHEN MATCHED THEN
            UPDATE SET
                data = source.data,
                failed_contract = source.failed_contract,
                contract_id = source.contract_id,
                contract_version = source.contract_version,
                datacontract_cli_version = source.datacontract_cli_version,
                validation_result = source.validation_result,
                violations = source.violations,
                raw_filepaths = source.raw_filepaths,
                flow_run_id = source.flow_run_id,
                datetime_validacao = source.datetime_validacao
        WHEN NOT MATCHED THEN
            INSERT (
                data,
                table_id,
                timestamp_captura,
                failed_contract,
                contract_id,
                contract_version,
                datacontract_cli_version,
                validation_result,
                violations,
                raw_filepaths,
                flow_run_id,
                datetime_validacao
            )
            VALUES (
                source.data,
                source.table_id,
                source.timestamp_captura,
                source.failed_contract,
                source.contract_id,
                source.contract_version,
                source.datacontract_cli_version,
                source.validation_result,
                source.violations,
                source.raw_filepaths,
                source.flow_run_id,
                source.datetime_validacao
            )
        """,
        job_config=job_config,
    ).result()


def _aggregate_result(results: list[ResultEnum]) -> ResultEnum:
    for result in (ResultEnum.error, ResultEnum.failed, ResultEnum.warning):
        if result in results:
            return result
    return ResultEnum.passed
