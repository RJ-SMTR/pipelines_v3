# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Optional

import pytz

from pipelines import constants as smtr_constants
from pipelines.default.generic_capture import constants
from pipelines.utils.fs import get_data_folder_path
from pipelines.utils.gcp.bigquery import SourceTable


class SourceCaptureContext:
    def __init__(
        self,
        source: SourceTable,
        timestamp: datetime,
        extra_parameters: Optional[dict] = None,
    ):
        self.source = source
        self.timestamp = timestamp.astimezone(tz=pytz.timezone(smtr_constants.TIMEZONE))
        self.extra_parameters = extra_parameters

        self.partition = self.get_partition()
        self.raw_filepath, self.source_filepath = self.get_filepaths()

        self.captured_raw_filepaths = []

    def get_partition(self) -> str:
        print("Criando partição...")
        print(f"Timestamp recebida: {self.timestamp}")

        partition = f"data={self.timestamp.strftime('%Y-%m-%d')}"
        if not self.source.partition_date_only:
            partition = f"{partition}/hora={self.timestamp.strftime('%H')}"

        print(f"Partição criada com sucesso: {partition}")

        return partition

    def get_filepaths(self) -> tuple[str, str]:
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
            ),
        )
