# -*- coding: utf-8 -*-
"""Tasks Prefect para importar e executar contratos de dados."""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.capture.data_contract.utils import (
    adapt_contract_schema,
    add_local_server,
    get_datacontract_command,
    get_default_contract_path,
    import_contract_if_missing,
    load_contract,
    run_datacontract,
    write_contract,
)


@task(cache_policy=NO_CACHE)
def validate_raw_data_contract(  # noqa: PLR0913
    manifest_path: str | Path,
    model_name: str,
    raw_filepaths: list[str],
    dataset_name: str | None = None,
    layer_name: str | None = None,
    contract_path: str | Path | None = None,
    file_format: str = "csv",
    ignored_columns: Iterable[str] | None = (),
    primary_keys: Iterable[str] | None = (),
) -> None:
    """Importa, adapta e testa o contrato contra os arquivos brutos capturados.

    Args:
        manifest_path (str | Path): Caminho do manifest dbt usado pelo importer.
        model_name (str): Nome do modelo dbt que representa o contrato.
        raw_filepaths (list[str]): Caminhos dos arquivos brutos capturados.
        dataset_name (str | None): Nome do dataset dbt, necessário quando
            ``contract_path`` não é informado.
        layer_name (str | None): Camada dbt que contém o modelo, necessária quando
            ``contract_path`` não é informado.
        contract_path (str | Path | None): Caminho persistente opcional; por padrão,
            usa ``queries/models/<dataset>/<camada>/data_contracts``.
        file_format (str): Formato aceito pelo servidor local do ODCS.
        ignored_columns (Iterable[str]): Colunas técnicas que não existem no bruto.
        primary_keys (Iterable[str]): Chaves primárias reais do source.

    Raises:
        ValueError: Se a captura não produziu arquivos brutos.
        RuntimeError: Se o CLI não estiver instalado ou o teste falhar.
    """
    if not raw_filepaths:
        raise ValueError(f"Nenhum arquivo bruto foi capturado para o modelo {model_name}.")

    datacontract_command = get_datacontract_command()
    manifest = Path(manifest_path)
    if contract_path:
        contract = Path(contract_path)
    else:
        if not dataset_name or not layer_name:
            raise ValueError(
                "dataset_name e layer_name são obrigatórios quando contract_path não é informado."
            )
        contract = get_default_contract_path(
            model_name=model_name,
            dataset_name=dataset_name,
            layer_name=layer_name,
        )
    imported = import_contract_if_missing(
        manifest_path=manifest,
        model_name=model_name,
        contract_path=contract,
        datacontract_command=datacontract_command,
    )
    imported_contract = load_contract(contract)
    if imported:
        imported_contract = adapt_contract_schema(
            imported_contract,
            ignored_columns=ignored_columns,
            primary_keys=primary_keys,
        )
        write_contract(imported_contract, contract)

    with TemporaryDirectory(prefix="datacontract-") as temporary_directory:
        for raw_filepath in raw_filepaths:
            runtime_contract = add_local_server(
                imported_contract,
                raw_filepath=raw_filepath,
                file_format=file_format,
            )
            runtime_contract_path = Path(temporary_directory) / "contract.yaml"
            write_contract(runtime_contract, runtime_contract_path)
            run_datacontract([datacontract_command, "test", str(runtime_contract_path)])
