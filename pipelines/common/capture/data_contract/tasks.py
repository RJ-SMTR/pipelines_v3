# -*- coding: utf-8 -*-
"""Tasks Prefect para baixar contratos versionados e validar os arquivos brutos."""

from pathlib import Path
from typing import Optional

import yaml
from datacontract.data_contract import DataContract
from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    adapt_contract_schema,
    add_local_server,
    contract_test_results,
    download_contracts_from_commit,
    format_test_results,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.google_chat import notify_test_failures_google_chat


@task(cache_policy=NO_CACHE)
def download_data_contracts(contexts: list[SourceCaptureContext], env: str) -> Path | None:
    """
    Baixa os contratos dos sources capturados para a validação dos arquivos.

    Args:
        contexts (list[SourceCaptureContext]): Contextos das fontes capturadas.
        env (str): Ambiente usado para selecionar a branch.

    Returns:
        Path | None: Diretório dos contratos, ou None quando nenhuma fonte os utiliza.
    """
    if not any(context.source.validate_data_contract for context in contexts):
        return None
    return download_contracts_from_commit(env)


@task(cache_policy=NO_CACHE)
def validate_raw_data_contract(
    context: SourceCaptureContext, contracts_dir: Path | None
) -> list[dict] | None:
    """
    Valida os arquivos brutos antes do upload usando uma cópia local do contrato.

    A reprovação não interrompe a task: os resultados são devolvidos para que
    check_data_contract notifique as falhas e barre o upload.

    Args:
        context (SourceCaptureContext): Fonte e arquivos brutos capturados.
        contracts_dir (Path | None): Diretório com os contratos baixados.

    Returns:
        list[dict] | None: Checks executados (ver contract_test_results), ou None quando a
            fonte não valida contrato.

    Raises:
        ValueError: Contrato não encontrado ou incompatível com a fonte.
    """
    source = context.source
    if not source.validate_data_contract:
        return None
    contract_path = next(contracts_dir.rglob(f"{source.data_contract_model}.odcs.yaml"), None)
    if contract_path is None:
        raise ValueError(
            f"Contrato {source.data_contract_model} não encontrado em {contracts_dir}."
        )
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    runtime_contract = adapt_contract_schema(
        contract,
        ignored_columns=source.data_contract_ignored_columns,
        primary_keys=source.primary_keys,
    )
    results = []
    for raw_filepath in context.captured_raw_filepaths:
        runtime_contract = add_local_server(
            runtime_contract, raw_filepath=raw_filepath, file_format=source.raw_filetype
        )
        result = DataContract(
            data_contract_str=yaml.safe_dump(runtime_contract),
            server="incoming",
            include_failed_samples=True,
        ).test()
        print(
            f"Testando {contract_path.name}\n"
            f"Servidor: incoming (caminho={raw_filepath})\n"
            f"{format_test_results(result)}"
        )
        results.extend(contract_test_results(result, table=source.table_id))
    return results


@task(cache_policy=NO_CACHE)
def check_data_contract(
    validations: list[list[dict] | None], env: str, webhook_key: Optional[str]
) -> None:
    """
    Notifica as falhas da validação do contrato e interrompe o flow antes do upload.

    Args:
        validations (list[list[dict] | None]): Retorno de validate_raw_data_contract por contexto.
        env (str): prod ou dev.
        webhook_key (Optional[str]): Chave do webhook do Google Chat no secret; sem chave, não
            notifica.

    Raises:
        ValueError: Algum arquivo bruto reprovou no contrato.
    """
    failures = [
        result
        for validation in validations
        if validation
        for result in validation
        if result["result"] in ("FAIL", "ERROR")
    ]
    notify_test_failures_google_chat(
        failures=failures,
        title=f"Contrato de dados - {runtime.flow_run.flow_name}",
        env=env,
        webhook_key=webhook_key,
    )
    if failures:
        tables = ", ".join(sorted({failure["table"] for failure in failures}))
        raise ValueError(
            f"Contrato de dados reprovado: {len(failures)} verificações falharam ({tables})."
        )
