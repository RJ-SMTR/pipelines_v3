# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import pandas as pd
from prefect import task

from pipelines import constants as smtr_constants
from pipelines.default.capture.generic_capture.utils import SourceCaptureContext
from pipelines.default.generic_capture import constants
from pipelines.utils.fs import read_raw_data, save_local_file
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.pretreatment import (
    create_timestamp_captura,
    transform_to_nested_structure,
)
from pipelines.utils.utils import convert_timezone, data_info_str


@task
def create_capture_contexts(  # noqa: PLR0913
    env: str,
    sources: list[SourceTable],
    timestamp: datetime,
    recapture: bool,
    recapture_days: int,
    recapture_timestamps: list[str],
):
    contexts = []
    for source in sources:
        if recapture:
            if recapture_timestamps:
                timestamps = [
                    convert_timezone(datetime.fromisoformat(t)) for t in recapture_timestamps
                ]
            else:
                timestamps = source.get_uncaptured_timestamps(
                    timestamp=timestamp,
                    retroactive_days=recapture_days,
                )
        else:
            timestamps = [timestamp]

        contexts += [
            SourceCaptureContext(source=source.set_env(env=env), timestamp=t) for t in timestamps
        ]

    return contexts


@task
def get_raw_data(context: SourceCaptureContext, data_extractor: Callable):
    """
    Faz a extração dos dados raw e salva localmente
    """

    captured_raw_filepaths = data_extractor()

    context.captured_raw_filepaths = captured_raw_filepaths


@task
def upload_raw_file_to_gcs(context: SourceCaptureContext):
    """
    Sobe os arquivos raw para o GCS
    """
    for path in context.captured_raw_filepaths:
        context.source.upload_raw_file(
            raw_filepath=path,
            partition=context.partition,
        )


@task
def transform_raw_to_nested_structure(context: SourceCaptureContext):
    """
    Task para aplicar pre-tratamentos e transformar os dados para o formato aninhado
    """
    csv_mode = "w"
    source = context.source
    timestamp = context.timestamp
    primary_keys = source.primary_keys
    source_filetype = constants.SOURCE_FILETYPE

    for raw_filepath in context.captured_raw_filepaths:
        data = read_raw_data(
            filepath=raw_filepath,
            reader_args=source.pretreatment_reader_args,
        )

        if data.empty:
            print("Dataframe vazio, pulando tratamento...")
            data = pd.DataFrame()
        else:
            print(f"Raw data:\n{data_info_str(data)}")

            data_columns_len = len(data.columns)
            captura = create_timestamp_captura(
                timestamp=datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE))
            )
            data["_datetime_execucao_flow"] = captura

            for step in source.pretreat_funcs:
                data = step(data=data, timestamp=timestamp, primary_keys=primary_keys)

            if len(primary_keys) < data_columns_len:
                data = transform_to_nested_structure(data=data, primary_keys=primary_keys)

            data["timestamp_captura"] = create_timestamp_captura(timestamp=timestamp)

        print(f"Estrutura aninhada criada! Dados: \n{data_info_str(data)}")

        source_filepath = context.source_filepath
        save_local_file(
            filepath=source_filepath,
            filetype=source_filetype,
            data=data,
            csv_mode=csv_mode,
        )
        csv_mode = "a"
        print(f"Dados salvos em {source_filepath}")


@task
def upload_source_data_to_gcs(context: SourceCaptureContext):
    """
    Sobe os dados aninhados para a pasta source do GCS
    """
    source = context.source
    source_filepath = context.source_filepath
    partition = context.partition

    print(f"Source: {source.table_full_name}")
    print(f"Timestamp: {context.timestamp}")

    if not source.exists():
        print("Tabela de staging não existe, criando tabela...")
        source.append(source_filepath=source_filepath, partition=partition)
        source.create(sample_filepath=source_filepath)
        print("Tabela de staging criada")
    else:
        print("Tabela de staging já existe, adicionando dados...")
        source.append(source_filepath=source_filepath, partition=partition)
        print("Dados adicionados")
