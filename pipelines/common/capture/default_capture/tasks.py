# -*- coding: utf-8 -*-
import io
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow.parquet as pq
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture.utils import (
    SourceCaptureContext,
    constants,
    persist_capture_log,
)
from pipelines.common.utils.fs import read_raw_data, save_local_file
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.gcp.storage import Storage
from pipelines.common.utils.pretreatment import (
    create_nested_content,
    create_timestamp_captura,
    run_pretreat_funcs,
)
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import convert_timezone, data_info_str


@task(cache_policy=NO_CACHE)
def create_capture_contexts(  # noqa: PLR0913
    env: str,
    sources: list[SourceTable],
    source_table_ids: Optional[list[str]],
    timestamp: datetime,
    recapture: bool,
    recapture_days: int,
    recapture_timestamps: list[str],
    extra_parameters: Optional[dict[str, dict]] = None,
) -> list[SourceCaptureContext]:
    """
    Cria os contextos de captura para cada fonte

    Args:
        env (str): prod ou dev.
        sources (list[SourceTable]): Lista de SourceTable para captura.
        source_table_ids (Optional[list[str]]): Lista de table_ids dos sources a serem capturados.
        timestamp (datetime): Timestamp de captura.
        recapture (bool): Indica se a execução é uma recaptura.
        recapture_days (int): Número de dias retroativos considerados na recaptura.
        recapture_timestamps (list[str]): Lista de timestamps a recapturar.
        extra_parameters (Optional[dict[str, dict]]): Parametros extras mapeados no padrão
            {"table_id": {"key": "value", ...}, ...}.

    Returns:
        list[SourceCaptureContext]: Lista de contextos de captura.
    """
    contexts = []

    if source_table_ids is None:
        sources = [s.set_env(env=env) for s in sources]
    else:
        sources = [s.set_env(env=env) for s in sources if s.table_id in source_table_ids]

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
            SourceCaptureContext(
                source=source,
                timestamp=t,
                extra_parameters=(
                    extra_parameters.get(source.table_id) if extra_parameters else None
                ),
            )
            for t in timestamps
        ]

    return contexts


@task(cache_policy=NO_CACHE, tags=["data-processing"])
def get_raw_data(context: SourceCaptureContext, data_extractor: Callable):
    """
    Extrai os dados brutos e salva os caminhos dos arquivos no contexto.

    Args:
        context (SourceCaptureContext): Contexto da captura.
        data_extractor (Callable): Função responsável por extrair e salvar os dados brutos.
    """

    try:
        captured_raw_filepaths = data_extractor()
    except Exception as error:
        persist_capture_log(context=context, success=False, error=error)
        raise

    persist_capture_log(context=context, success=True)

    context.captured_raw_filepaths = captured_raw_filepaths


@task(cache_policy=NO_CACHE)
def upload_raw_file_to_gcs(context: SourceCaptureContext, if_exists: str = "replace"):
    """
    Envia os arquivos brutos para o GCS.

    Args:
        context (SourceCaptureContext): Contexto da captura após a definição
            do atributo captured_raw_filepaths.
        if_exists (str): Ação a ser tomada caso o arquivo exista
                no storage (raise, pass, replace)
    """
    for path in context.captured_raw_filepaths:
        context.source.upload_raw_file(
            raw_filepath=path,
            partition=context.partition,
            if_exists=if_exists,
        )


def transform_raw_chunk(
    data: pl.DataFrame, context: SourceCaptureContext, captura: str
) -> pl.DataFrame:
    """
    Aplica pré-tratamentos e transforma um bloco de dados brutos em estrutura aninhada.

    Args:
        data (pl.DataFrame): Bloco de dados brutos.
        context (SourceCaptureContext): Contexto da captura.
        captura (str): Timestamp de execução do flow.

    Returns:
        pl.DataFrame: Dados em estrutura aninhada.
    """
    print(f"Raw data:\n{data_info_str(data)}")
    primary_keys = context.source.primary_keys
    data_columns_len = len(data.columns)

    data = run_pretreat_funcs(
        data=data,
        pretreat_funcs=context.source.pretreat_funcs,
        context=context,
    )

    data = data.with_columns(pl.lit(captura).alias("_datetime_execucao_flow"))

    if len(primary_keys) < data_columns_len:
        data = create_nested_content(data=data, primary_keys=primary_keys)

    return data.with_columns(
        pl.lit(create_timestamp_captura(timestamp=context.timestamp)).alias("timestamp_captura")
    )


@task(cache_policy=NO_CACHE, tags=["data-processing"])
def transform_raw_to_nested_structure(context: SourceCaptureContext):
    """
    Aplica pré-tratamentos e transforma os dados brutos em estrutura aninhada.

    Args:
        context (SourceCaptureContext): Contexto da captura após a definição
            do atributo captured_raw_filepaths.
    """
    csv_mode = "w"
    reader_args = context.source.pretreatment_reader_args or {}
    parquet_writer = None

    for raw_filepath in context.captured_raw_filepaths:
        captura = create_timestamp_captura(
            timestamp=datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE))
        )

        for chunk in read_raw_data(filepath=raw_filepath, reader_args=reader_args):
            if chunk.is_empty():
                print("Bloco vazio, pulando tratamento...")
                continue

            data = transform_raw_chunk(data=chunk, context=context, captura=captura)
            print(f"Estrutura aninhada criada! Dados: \n{data_info_str(data)}")

            save_local_file(
                filepath=context.source_filepath,
                filetype=constants.SOURCE_FILETYPE,
                data=data,
                csv_mode=csv_mode,
            )
            csv_mode = "a"
            print(f"Dados salvos em {context.source_filepath}")

            if context.source.parquet_source:
                parquet_writer = write_parquet_chunk(
                    data=data, filepath=context.source_parquet_filepath, writer=parquet_writer
                )

    if parquet_writer is not None:
        parquet_writer.close()
        print(f"Dados salvos em {context.source_parquet_filepath}")

    if csv_mode == "w":
        print("Nenhum dado encontrado, salvando arquivo vazio")
        save_local_file(
            filepath=context.source_filepath,
            filetype=constants.SOURCE_FILETYPE,
            data=pl.DataFrame(),
        )


def write_parquet_chunk(
    data: pl.DataFrame,
    filepath: str,
    writer: Optional[pq.ParquetWriter],
) -> pq.ParquetWriter:
    """
    Grava um bloco do source como um row group do arquivo Parquet, com todas as colunas
    como texto

    Args:
        data (pl.DataFrame): Bloco de dados em estrutura aninhada
        filepath (str): Caminho do arquivo Parquet
        writer (Optional[pq.ParquetWriter]): Writer aberto no bloco anterior

    Returns:
        pq.ParquetWriter: Writer aberto, a ser fechado após o último bloco
    """
    table = data.cast(pl.String).to_arrow()
    if writer is None:
        writer = pq.ParquetWriter(
            filepath,
            table.schema,
            compression="zstd",
            compression_level=constants.SOURCE_PARQUET_COMPRESSION_LEVEL,
        )
    writer.write_table(table)
    return writer


@task(cache_policy=NO_CACHE)
def upload_source_data_to_gcs(context: SourceCaptureContext, if_exists: str = "replace"):
    """
    Envia os dados aninhados para a pasta source do GCS.

    Args:
        context (SourceCaptureContext): Contexto da captura.
        if_exists (str): Ação a ser tomada caso o arquivo exista
                no storage (raise, pass, replace)
    """

    source = context.source
    source_filepath = context.source_filepath
    partition = context.partition

    print(f"Source: {source.table_full_name}")
    print(f"Timestamp: {context.timestamp}")

    if not source.exists():
        print("Tabela de staging não existe, criando tabela...")
        source.append(source_filepath=source_filepath, partition=partition, if_exists=if_exists)
        source.create(sample_filepath=source_filepath)
        print("Tabela de staging criada")
    else:
        print("Tabela de staging já existe, adicionando dados...")
        source.append(source_filepath=source_filepath, partition=partition, if_exists=if_exists)
        print("Dados adicionados")

    if source.parquet_source:
        upload_source_parquet(context=context, if_exists=if_exists)


def upload_source_parquet(context: SourceCaptureContext, if_exists: str):
    """
    Envia o source em Parquet para a pasta source_parquet do GCS e cria a tabela externa
    se ela não existir

    Args:
        context (SourceCaptureContext): Contexto da captura.
        if_exists (str): Ação a ser tomada caso o arquivo exista
                no storage (raise, pass, replace)
    """
    if not Path(context.source_parquet_filepath).exists():
        print("Nenhum dado em Parquet, pulando upload")
        return

    source = context.source
    source.append(
        source_filepath=context.source_parquet_filepath,
        partition=context.partition,
        if_exists=if_exists,
        mode="source_parquet",
    )
    if not source.get_parquet_table().exists():
        print("Tabela Parquet não existe, criando tabela...")
        source.create_parquet(sample_filepath=context.source_filepath)
        print("Tabela Parquet criada")


@task(cache_policy=NO_CACHE)
def save_capture_datetime_redis(capture_datetime: dict) -> None:
    """Persiste no Redis o marcador de captura indicado pelo flow."""
    redis_client = get_redis_client()
    redis_client.set(capture_datetime["key"], capture_datetime["value"])
    print(f"Marcador de captura salvo no Redis: {capture_datetime['key']}")


@task(cache_policy=NO_CACHE)
def get_raw_from_gcs(
    context: SourceCaptureContext,
    table_id: str,
):
    """
    Busca dados do GCS como fallback (backup/upload manual).

    Usado quando a extração principal (FTP, API, etc) retorna vazio.

    Args:
        context (SourceCaptureContext): Contexto de captura com informações de fonte e timestamp
        table_id (str): ID da tabela para montar o nome do arquivo

    Returns:
        list[str]: Lista com caminho do arquivo salvo localmente, ou lista vazia se não encontrar
    """
    try:
        storage = Storage(
            env=context.source.env,
            dataset_id=context.source.dataset_id,
            table_id=context.source.table_id,
        )

        # Monta o path esperado no GCS
        filename = f"{table_id}_{context.timestamp.strftime('%Y%m%d')}"
        blob_path = f"upload/{context.source.dataset_id}/{context.source.table_id}/{filename}.zip"

        blob = storage.bucket.get_blob(blob_path)

        if blob is None:
            print(f"[GCS] Arquivo não encontrado: {blob_path}")
            return []

        # Baixa e extrai o ZIP
        data = blob.download_as_bytes()
        with zipfile.ZipFile(io.BytesIO(data), "r") as zipped_file:
            raw_text = zipped_file.read(f"{filename}.txt").decode("utf-8")

        # Salva localmente como texto bruto (igual ao get_raw_ftp)
        filepath = context.raw_filepath.format(page=0)
        save_local_file(filepath=filepath, filetype="txt", data=raw_text)

        print(f"[GCS] Dados carregados com sucesso: {filepath}")
        return [filepath]

    except Exception as e:
        print(f"[GCS] Erro ao buscar dados: {e}")
        return []
