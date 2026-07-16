# -*- coding: utf-8 -*-
from pathlib import Path

from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    ContractValidationResult,
    persist_contract_validation_result,
    validate_contract_files,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


@task(cache_policy=NO_CACHE, tags=["data-processing"])
def validate_data_contract(
    context: SourceCaptureContext,
) -> ContractValidationResult:
    contract_path = context.extra_parameters["data_contract_path"]
    validation = validate_contract_files(
        contract_path=contract_path,
        raw_filepaths=context.captured_raw_filepaths,
    )
    validation.raw_filepaths = [
        (
            f"gs://{context.source.bucket_name}/raw/{context.source.dataset_id}/"
            f"{context.source.table_id}/{context.partition}/{Path(filepath).name}"
        )
        for filepath in context.captured_raw_filepaths
    ]

    flow_run_id = str(runtime.flow_run.id) if runtime.flow_run.id is not None else None
    persist_contract_validation_result(
        context=context,
        validation=validation,
        flow_run_id=flow_run_id,
    )

    status = "violado" if validation.failed_contract else "válido"
    print(
        f"Contrato {validation.contract_id} v{validation.contract_version} {status} "
        f"para {context.source.table_id} em {context.timestamp.isoformat()}"
    )
    if validation.violations:
        print(f"Violações: {validation.violations}")

    return validation
