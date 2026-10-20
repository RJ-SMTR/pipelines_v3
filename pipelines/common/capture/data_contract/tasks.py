# -*- coding: utf-8 -*-
"""Tasks Prefect para baixar contratos versionados e validar os arquivos brutos."""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import yaml
from datacontract.data_contract import DataContract
from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    adapt_contract_schema,
    add_local_server,
    add_referenced_schema,
    contract_test_results,
    download_contracts_from_commit,
    format_test_results,
    keep_relationship_properties,
    link_schema_files,
    load_contract,
    prepare_raw_file,
    referenced_schema_names,
    remove_relationships,
    remove_type_checks,
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
    check_data_contract notifique as falhas e barre o upload. Os relationships são removidos,
    pois dependem de outras fontes e são validados em validate_data_contract_relationships.

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
    contract_path, contract = load_contract(contracts_dir, source.data_contract_model)
    runtime_contract = adapt_contract_schema(
        remove_relationships(contract),
        ignored_columns=source.data_contract_ignored_columns,
        primary_keys=source.primary_keys,
    )
    results = []
    for raw_filepath in context.captured_raw_filepaths:
        with TemporaryDirectory() as folder:
            server_path, file_format = prepare_raw_file(
                raw_filepath, Path(folder) / "raw", source.raw_filetype
            )
            runtime_contract = add_local_server(
                runtime_contract, raw_filepath=server_path, file_format=file_format
            )
            result = DataContract(
                data_contract_str=yaml.safe_dump(runtime_contract),
                server="incoming",
                include_failed_samples=True,
            ).test()
        remove_type_checks(result)
        print(
            f"Testando {contract_path.name}\n"
            f"Servidor: incoming (caminho={raw_filepath})\n"
            f"{format_test_results(result)}"
        )
        results.extend(contract_test_results(result, table=source.table_id))
    return results


@task(cache_policy=NO_CACHE)
def validate_data_contract_relationships(
    contexts: list[SourceCaptureContext],
    contracts_dir: Path | None,
    validations: list[list[dict] | None],
) -> list[dict] | None:
    """
    Valida os relationships dos contratos com os arquivos brutos das fontes referenciadas.

    Cada fonte com relationships é testada junto com os arquivos das fontes referenciadas,
    capturadas na mesma execução, e só os checks de relationship são devolvidos. Fontes com
    ERROR em validate_raw_data_contract (arquivo ilegível, por exemplo) ficam de fora, como
    origem e como referência, para não repetir a mesma causa na notificação. Uma fonte
    referenciada que não foi capturada gera um check de relationship com alerta.

    Args:
        contexts (list[SourceCaptureContext]): Contextos das fontes capturadas.
        contracts_dir (Path | None): Diretório com os contratos baixados.
        validations (list[list[dict] | None]): Resultados de validate_raw_data_contract, na
            mesma ordem de contexts.

    Returns:
        list[dict] | None: Checks de relationship (ver contract_test_results), ou None quando
            nenhuma fonte declara relationships.

    Raises:
        ValueError: Contrato da fonte ou da fonte referenciada não encontrado.
    """
    if contracts_dir is None:
        return None
    error_models = {
        context.source.data_contract_model
        for context, validation in zip(contexts, validations, strict=True)
        if any(result["result"] == "ERROR" for result in validation or [])
    }
    contexts_by_model = {context.source.data_contract_model: context for context in contexts}
    results = []
    for context in contexts:
        source = context.source
        if not source.validate_data_contract or source.data_contract_model in error_models:
            continue
        contract_path, contract = load_contract(contracts_dir, source.data_contract_model)
        runtime_contract = adapt_contract_schema(
            remove_relationships(contract, schema_names=error_models),
            ignored_columns=source.data_contract_ignored_columns,
            primary_keys=source.primary_keys,
        )
        schema_names = referenced_schema_names(runtime_contract)
        if not schema_names:
            continue
        files_by_schema = {source.data_contract_model: context.captured_raw_filepaths}
        for schema_name in sorted(schema_names):
            referenced_context = contexts_by_model.get(schema_name)
            if referenced_context is None:
                print(f"{schema_name} não capturado nesta execução; relationship não verificado.")
                continue
            _, referenced_contract = load_contract(contracts_dir, schema_name)
            runtime_contract = add_referenced_schema(
                runtime_contract,
                adapt_contract_schema(
                    referenced_contract,
                    ignored_columns=referenced_context.source.data_contract_ignored_columns,
                ),
            )
            files_by_schema[schema_name] = referenced_context.captured_raw_filepaths
        with TemporaryDirectory() as folder:
            server_path, file_format = link_schema_files(
                Path(folder), files_by_schema, source.raw_filetype
            )
            runtime_contract = add_local_server(
                keep_relationship_properties(runtime_contract),
                raw_filepath=server_path,
                file_format=file_format,
            )
            result = DataContract(
                data_contract_str=yaml.safe_dump(runtime_contract),
                server="incoming",
                include_failed_samples=True,
            ).test()
        result.checks = [check for check in result.checks if check.type == "field_relationships"]
        result.calculate_result()
        print(
            f"Testando relationships de {contract_path.name}\n"
            f"Schemas: {', '.join(sorted(files_by_schema))}\n"
            f"{format_test_results(result)}"
        )
        results.extend(contract_test_results(result, table=source.table_id))
    return results or None


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
