# -*- coding: utf-8 -*-
"""Tasks Prefect para baixar contratos versionados e validar os arquivos brutos."""

from pathlib import Path
from typing import Optional

from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    adapt_contract_schema,
    add_referenced_schema,
    contract_test_results,
    download_contracts_from_commit,
    format_test_results,
    keep_checks,
    keep_relationship_properties,
    load_contract,
    referenced_schema_names,
    remove_relationships,
    run_contract_test,
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
        result = run_contract_test(
            runtime_contract, {source.data_contract_model: [raw_filepath]}, source.raw_filetype
        )
        print(
            f"Testando {contract_path.name}\n"
            f"Servidor: incoming (caminho={raw_filepath})\n"
            f"{format_test_results(result)}"
        )
        results.extend(
            contract_test_results(result, table=source.table_id, partition=context.partition)
        )
    return results


def add_referenced_sources(
    contract: dict,
    context: SourceCaptureContext,
    contexts_by_source: dict[tuple[str, str], SourceCaptureContext],
    contracts_dir: Path,
) -> tuple[dict, dict[str, list[str]]]:
    """
    Anexa ao contrato os schemas referenciados pelos relationships e reúne os arquivos brutos.

    As referências vêm da mesma partição da fonte: numa recaptura, cada partição é validada
    com os arquivos da sua própria captura. Um schema referenciado sem fonte capturada nessa
    partição não é anexado; o datacontract gera um check de relationship com alerta para ele.

    Args:
        contract (dict): Contrato da fonte, já adaptado.
        context (SourceCaptureContext): Contexto da fonte que declara os relationships.
        contexts_by_source (dict[tuple[str, str], SourceCaptureContext]): Contextos da
            execução por modelo e partição.
        contracts_dir (Path): Diretório com os contratos baixados.

    Returns:
        tuple[dict, dict[str, list[str]]]: Contrato com os schemas referenciados e arquivos
            brutos de cada schema.
    """
    files_by_schema = {context.source.data_contract_model: context.captured_raw_filepaths}
    for schema_name in sorted(referenced_schema_names(contract)):
        referenced_context = contexts_by_source.get((schema_name, context.partition))
        if referenced_context is None:
            print(f"{schema_name} sem captura em {context.partition}; relationship não verificado.")
            continue
        _, referenced_contract = load_contract(contracts_dir, schema_name)
        contract = add_referenced_schema(
            contract,
            adapt_contract_schema(
                referenced_contract,
                ignored_columns=referenced_context.source.data_contract_ignored_columns,
            ),
        )
        files_by_schema[schema_name] = referenced_context.captured_raw_filepaths
    return contract, files_by_schema


@task(cache_policy=NO_CACHE)
def validate_data_contract_relationships(
    contexts: list[SourceCaptureContext],
    contracts_dir: Path | None,
    validations: list[list[dict] | None],
) -> list[dict] | None:
    """
    Valida os relationships dos contratos com os arquivos brutos das fontes referenciadas.

    Cada fonte com relationships é testada junto com os arquivos das fontes referenciadas,
    capturadas na mesma execução e na mesma partição, e só os checks de relationship são
    devolvidos. Fontes com ERROR em validate_raw_data_contract (arquivo ilegível, por exemplo)
    ficam de fora naquela partição, como origem e como referência, para não repetir a mesma
    causa na notificação. Uma fonte referenciada que não foi capturada na partição gera um
    check de relationship com alerta.

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
    error_sources = {
        (context.source.data_contract_model, context.partition)
        for context, validation in zip(contexts, validations, strict=True)
        if any(result["result"] == "ERROR" for result in validation or [])
    }
    contexts_by_source = {
        (context.source.data_contract_model, context.partition): context for context in contexts
    }
    results = []
    for context in contexts:
        source = context.source
        if not source.validate_data_contract or (
            (source.data_contract_model, context.partition) in error_sources
        ):
            continue
        error_models = {
            model for model, partition in error_sources if partition == context.partition
        }
        contract_path, contract = load_contract(contracts_dir, source.data_contract_model)
        runtime_contract = adapt_contract_schema(
            remove_relationships(contract, schema_names=error_models),
            ignored_columns=source.data_contract_ignored_columns,
            primary_keys=source.primary_keys,
        )
        if not referenced_schema_names(runtime_contract):
            continue
        runtime_contract, files_by_schema = add_referenced_sources(
            runtime_contract, context, contexts_by_source, contracts_dir
        )
        result = run_contract_test(
            keep_relationship_properties(runtime_contract), files_by_schema, source.raw_filetype
        )
        keep_checks(result, lambda check: check.type == "field_relationships")
        print(
            f"Testando relationships de {contract_path.name}\n"
            f"Schemas: {', '.join(sorted(files_by_schema))}\n"
            f"{format_test_results(result)}"
        )
        results.extend(
            contract_test_results(result, table=source.table_id, partition=context.partition)
        )
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
