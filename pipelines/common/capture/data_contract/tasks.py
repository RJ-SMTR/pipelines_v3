# -*- coding: utf-8 -*-
"""Tasks Prefect para baixar contratos versionados e validar os arquivos brutos."""

from pathlib import Path

import yaml
from datacontract.data_contract import DataContract
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    add_local_server,
    download_contracts_from_commit,
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
def validate_raw_data_contract(context: SourceCaptureContext, contracts_dir: Path | None) -> None:
    """
    Valida os arquivos brutos antes do upload usando uma cópia local do contrato.

    Args:
        context (SourceCaptureContext): Fonte e arquivos brutos capturados.
        contracts_dir (Path | None): Diretório com os contratos baixados.

    Returns:
        None: A validação é concluída por efeito ou interrompe o flow com uma exceção.

    Raises:
        ValueError: Arquivos ausentes, contrato incompatível ou validação reprovada.
    """
    source = context.source
    if not source.validate_data_contract:
        return None
    if not context.captured_raw_filepaths:
        raise ValueError(f"Nenhum arquivo bruto foi capturado para {source.table_id}.")

    contract_path = next(contracts_dir.rglob(f"{source.data_contract_model}.odcs.yaml"))
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    for raw_filepath in context.captured_raw_filepaths:
        runtime_contract = add_local_server(
            contract, raw_filepath=raw_filepath, file_format=source.raw_filetype
        )
        result = DataContract(
            data_contract_str=yaml.safe_dump(runtime_contract), server="incoming"
        ).test()
        if not result.has_passed():
            raise ValueError(f"Falha no contrato de {raw_filepath}: {result.model_dump_json()}")
        print(f"Contrato validado: {raw_filepath} ({len(result.checks)} verificações)")
