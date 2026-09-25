# -*- coding: utf-8 -*-
"""Tasks Prefect para baixar contratos versionados e validar os arquivos brutos."""

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.repository import (
    contract_relative_path,
    download_contract_snapshot,
)
from pipelines.common.capture.data_contract.utils import (
    add_local_server,
    run_datacontract,
    write_contract,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


@task(cache_policy=NO_CACHE)
def prepare_data_contracts(contexts: list[SourceCaptureContext], env: str) -> dict[str, Any]:
    """Baixa uma cópia por contrato e fixa o SHA para a validação dos arquivos."""
    paths = [
        contract_relative_path(context.source.source_name, context.source.data_contract_model)
        for context in contexts
        if context.source.validate_data_contract
    ]
    return download_contract_snapshot(paths, env)


@task(cache_policy=NO_CACHE)
def validate_raw_data_contract(
    context: SourceCaptureContext, snapshot: dict[str, Any]
) -> dict[str, Any] | None:
    """Valida cada bruto contra a mesma versão, alterando apenas o servidor local.

    A geração e a adaptação estável do schema ocorrem na CI. Esta task não depende
    de queries/manifest nem modifica o contrato baixado. Retorna sua proveniência
    e propaga falhas, bloqueando os uploads que dependem dela.
    """
    source = context.source
    if not source.validate_data_contract:
        return None
    if not context.captured_raw_filepaths:
        raise ValueError(f"Nenhum arquivo bruto foi capturado para {source.table_id}.")
    path = contract_relative_path(source.source_name, source.data_contract_model)
    artifact = snapshot["contracts"][path]
    contract = artifact["contract"]
    schemas = contract.get("schema", [])
    if len(schemas) != 1 or schemas[0].get("name") != source.data_contract_model:
        raise ValueError(f"Schema inesperado no contrato {path}.")
    provenance = {
        "repository": snapshot["repository"],
        "ref": snapshot["ref"],
        "sha": snapshot["sha"],
        "path": path,
        "sha256": artifact["sha256"],
        "source": f"{source.source_name}.{source.table_id}",
    }
    print(f"Validando bruto com contrato: {provenance}")
    with TemporaryDirectory(prefix="datacontract-") as directory:
        for raw_filepath in context.captured_raw_filepaths:
            runtime_contract = add_local_server(
                contract, raw_filepath=raw_filepath, file_format=source.raw_filetype
            )
            runtime_path = Path(directory) / "contract.yaml"
            write_contract(runtime_contract, runtime_path)
            run_datacontract(
                [
                    sys.executable,
                    "-c",
                    "from datacontract.cli import main; main()",
                    "test",
                    str(runtime_path),
                    "--server",
                    "incoming",
                ]
            )
    return {**provenance, "status": "passed", "files": list(context.captured_raw_filepaths)}
