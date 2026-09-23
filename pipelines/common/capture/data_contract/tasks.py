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
    generate_contract,
    get_contract_path_from_manifest,
    get_datacontract_python,
    load_contract,
    run_datacontract,
    write_contract,
)
from pipelines.common.capture.default_capture.utils import SourceCaptureContext


@task(cache_policy=NO_CACHE)
def validate_raw_data_contract(
    context: SourceCaptureContext,
    manifest_path: str | Path,
    model_name: str | None = None,
    contract_path: str | Path | None = None,
    ignored_columns: Iterable[str] | None = (),
) -> None:
    """Importa, adapta e testa o contrato contra os arquivos brutos capturados.

    Args:
        context (SourceCaptureContext): Contexto após a extração do arquivo bruto.
        manifest_path (str | Path): Caminho do manifest dbt usado pelo importer.
        model_name (str | None): Nome do modelo dbt. Quando omitido, usa
            ``base_<context.source.table_id>``.
        contract_path (str | Path | None): Caminho do ODCS gerado, reescrito a cada execução.
            Por padrão, é resolvido pelo ``original_file_path`` do modelo no manifest.
        ignored_columns (Iterable[str]): Colunas técnicas que não existem no bruto.

    Raises:
        ValueError: Se a captura não produziu arquivos brutos.
        RuntimeError: Se o CLI não estiver instalado ou o teste falhar.
    """
    raw_filepaths = context.captured_raw_filepaths
    model_name = model_name or f"base_{context.source.table_id}"
    primary_keys = context.source.primary_keys or ()
    file_format = context.source.raw_filetype

    if not raw_filepaths:
        raise ValueError(f"Nenhum arquivo bruto foi capturado para o modelo {model_name}.")

    datacontract_python = get_datacontract_python()
    manifest = Path(manifest_path)
    if contract_path:
        contract = Path(contract_path)
    else:
        contract = get_contract_path_from_manifest(
            manifest_path=manifest,
            model_name=model_name,
        )
    with TemporaryDirectory(prefix="datacontract-") as temporary_directory:
        generated_contract = Path(temporary_directory) / "generated.odcs.yaml"
        generate_contract(
            manifest_path=manifest,
            model_name=model_name,
            contract_path=generated_contract,
            datacontract_python=datacontract_python,
        )
        imported_contract = adapt_contract_schema(
            load_contract(generated_contract),
            ignored_columns=ignored_columns,
            primary_keys=primary_keys,
        )
        write_contract(imported_contract, contract)

        for raw_filepath in raw_filepaths:
            runtime_contract = add_local_server(
                imported_contract,
                raw_filepath=raw_filepath,
                file_format=file_format,
            )
            runtime_contract_path = Path(temporary_directory) / "contract.yaml"
            write_contract(runtime_contract, runtime_contract_path)
            run_datacontract(
                [
                    datacontract_python,
                    "-c",
                    "from datacontract.cli import main; main()",
                    "test",
                    str(runtime_contract_path),
                    "--server",
                    "incoming",
                ]
            )
