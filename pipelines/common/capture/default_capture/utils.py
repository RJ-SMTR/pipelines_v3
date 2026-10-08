# -*- coding: utf-8 -*-
import re
from datetime import datetime
from typing import NamedTuple, Optional

import pandas as pd
import pytz
from prefect import runtime

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture import constants
from pipelines.common.utils.fs import create_partition, get_data_folder_path, save_local_file
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.utils import convert_timezone


class ShouldCapture(NamedTuple):
    """
    Resultado de um `should_capture_task`.

    Attributes:
        value (bool): Se True, o builder prossegue com a captura; se False, retorna cedo.
        payload (Optional[dict]): Payload livre do pipeline (ex.: hash da fonte) propagada para
            os passos seguintes do flow (janela de materialização, persistência de estado etc.).
    """

    value: bool
    payload: Optional[dict] = None


class SourceCaptureContext:
    def __init__(
        self,
        source: SourceTable,
        timestamp: datetime,
        extra_parameters: Optional[dict] = None,
    ):
        """
        Objeto contendo as informações básicas para captura de dados.

        Args:
            source (SourceTable): SourceTable da captura.
            timestamp (datetime): Timestamp da captura.
            extra_parameters (Optional[dict]): Parâmetros adicionais opcionais.
        """
        self.source = source
        self.timestamp = timestamp.astimezone(tz=pytz.timezone(smtr_constants.TIMEZONE))
        self.extra_parameters = extra_parameters

        self.partition = self.get_partition()
        self.raw_filepath, self.source_filepath = self.get_filepaths()

        self.captured_raw_filepaths = []

    def get_partition(self) -> str:
        """
        Gera a partição no formato Hive correspondente ao timestamp da captura.

        Returns:
            str: Partição formatada.
        """
        return create_partition(
            timestamp=self.timestamp,
            partition_date_only=self.source.partition_date_only,
        )

    def get_filepaths(self) -> tuple[str, str]:
        """
        Gera os caminhos de arquivo para raw e source.

        Returns:
            tuple[str, str]: Caminhos dos arquivos raw e source.
        """
        print("Criando filepaths...")
        data_folder = get_data_folder_path()
        print(f"Data folder: {data_folder}")
        filename = self.timestamp.strftime(constants.FILENAME_PATTERN)

        return (
            f"{data_folder}/"
            + constants.RAW_FILEPATH_PATTERN.format(
                dataset_id=self.source.dataset_id,
                table_id=self.source.table_id,
                partition=self.partition,
                filename=f"{filename}_{{page}}",
                filetype=self.source.raw_filetype,
            )
        ), (
            f"{data_folder}/"
            + constants.SOURCE_FILEPATH_PATTERN.format(
                dataset_id=self.source.dataset_id,
                table_id=self.source.table_id,
                partition=self.partition,
                filename=filename,
                filetype=self.source.raw_filetype,
            )
        )


def format_error(error: Exception) -> str:
    """
    Formata uma exceção para o log de captura, removendo query strings de URLs.

    Args:
        error (Exception): Exceção da extração.

    Returns:
        str: Mensagem formatada.
    """
    message = f"{type(error).__name__}: {error}"
    return re.sub(r"(/[^\s?]*)\?[^\s)'\"]+", r"\1?[REDACTED]", message)


def persist_capture_log(
    context: SourceCaptureContext,
    success: bool,
    error: Optional[Exception] = None,
):
    """
    Persiste o resultado de uma extração na tabela externa de logs da fonte.

    Args:
        context (SourceCaptureContext): Contexto da captura.
        success (bool): Se a extração foi concluída com sucesso.
        error (Optional[Exception]): Exceção da extração, caso tenha falhado.
    """
    logs_table = context.source.get_logs_table()
    timestamp_captura = datetime.now(tz=pytz.timezone(smtr_constants.TIMEZONE))

    filepath = f"{get_data_folder_path()}/" + constants.SOURCE_FILEPATH_PATTERN.format(
        dataset_id=logs_table.dataset_id,
        table_id=logs_table.table_id,
        partition=context.partition,
        filename=timestamp_captura.strftime(f"{constants.FILENAME_PATTERN}-%f"),
    )
    data = pd.DataFrame(
        [
            {
                "timestamp_captura": timestamp_captura,
                "sucesso": success,
                "erro": format_error(error) if error is not None else None,
            }
        ]
    )
    save_local_file(filepath=filepath, filetype="csv", data=data)

    if not logs_table.exists():
        logs_table.append(source_filepath=filepath, partition=context.partition)
        logs_table.create(sample_filepath=filepath)
    else:
        logs_table.append(source_filepath=filepath, partition=context.partition)


def rename_capture_flow_run() -> str:
    """
    Gera o nome para execução de flows de captura.

    Returns:
        str: Nome para execução do flow.
    """
    scheduled_start_time = convert_timezone(runtime.flow_run.scheduled_start_time).strftime(
        "%Y-%m-%d %H-%M-%S"
    )

    flow_name = runtime.flow_run.flow_name
    recapture = runtime.flow_run.parameters.get("recapture", False)
    return f"[{scheduled_start_time}] {flow_name} - Recapture: {recapture}"
