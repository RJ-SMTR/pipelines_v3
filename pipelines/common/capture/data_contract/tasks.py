# -*- coding: utf-8 -*-
"""Tasks Prefect para baixar contratos versionados e validar os arquivos brutos."""

from pathlib import Path

import yaml
from datacontract.data_contract import DataContract
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    adapt_contract_schema,
    add_local_server,
    contract_test_results,
    download_contracts_from_commit,
    format_test_results,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


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
) -> dict | None:
    """
    Valida os arquivos brutos antes do upload usando uma cópia local do contrato.

    A reprovação não interrompe a task: os erros são devolvidos para que o resultado seja
    notificado antes de raise_data_contract_failures barrar o upload.

    Args:
        context (SourceCaptureContext): Fonte e arquivos brutos capturados.
        contracts_dir (Path | None): Diretório com os contratos baixados.

    Returns:
        dict | None: Checks executados (results) e mensagens das reprovações (errors), ou None
            quando a fonte não valida contrato.

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
    validation = {"results": [], "errors": []}
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
            f"Testing {contract_path.name}\n"
            f"Server: incoming (path={raw_filepath})\n"
            f"{format_test_results(result)}"
        )
        validation["results"].extend(contract_test_results(result, table=source.table_id))
        if result.result not in ("passed", "warning"):
            failed_checks = [
                check
                for check in result.checks
                if check.result not in ("passed", "skipped", "warning")
            ]
            validation["errors"].append(
                f"Falha no contrato de {raw_filepath}: "
                f"{len(failed_checks)} de {len(result.checks)} checks falharam."
            )
            continue
        print(f"Contrato validado: {raw_filepath} ({len(result.checks)} verificações).")
    return validation


@task(cache_policy=NO_CACHE)
def raise_data_contract_failures(validations: list[dict | None]) -> None:
    """
    Interrompe o flow antes do upload quando algum arquivo bruto reprovou no contrato.

    Args:
        validations (list[dict | None]): Retorno de validate_raw_data_contract por contexto.

    Raises:
        ValueError: Algum arquivo bruto reprovou no contrato.
    """
    errors = [error for validation in validations if validation for error in validation["errors"]]
    if errors:
        raise ValueError("\n".join(errors))
