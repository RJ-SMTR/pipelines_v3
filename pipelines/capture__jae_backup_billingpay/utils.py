# -*- coding: utf-8 -*-
"""Utilidades para backup incremental de dados BillingPay"""

from datetime import datetime
from pathlib import Path
from typing import Union

from prefect import runtime
from pytz import timezone

from pipelines.capture__jae_backup_billingpay import constants
from pipelines.common.capture.default_capture import constants as capture_constants
from pipelines.common.treatment.default_treatment import (
    constants as treatment_constants,
)
from pipelines.common.utils.extractors.db import get_raw_db
from pipelines.common.utils.fs import get_data_folder_path, save_local_file
from pipelines.common.utils.redis import get_redis_client


def get_flow_run_name(database_name: str) -> str:
    """
    Gera o nome da run do Flow com base no nome do banco de dados

    Args:
        database_name (str): Nome do banco de dados

    Returns:
        str: Nome formatado para a run do flow
    """
    start_time = runtime.flow_run.start_time
    return f"{database_name}: {start_time.isoformat()}"


def create_partition(
    timestamp: datetime,
    partition_date_only: bool = True,
) -> str:
    """
    Cria a partição Hive de acordo com a timestamp

    Args:
        timestamp (datetime): timestamp de referência
        partition_date_only (bool): True se o particionamento deve ser feito apenas por data
            False se o particionamento deve ser feito por data e hora

    Returns:
        str: string com o particionamento
    """
    print("Creating file partition...")
    print(f"Timestamp received: {timestamp}")
    timestamp = timestamp.astimezone(tz=constants.TIMEZONE)
    print(f"Timestamp converted to {constants.TIMEZONE}: {timestamp}")
    partition = f"data={timestamp.strftime('%Y-%m-%d')}"
    if not partition_date_only:
        partition += f"&hora={timestamp.strftime('%H')}"
    print(f"Partition created: {partition}")
    return partition


def create_billingpay_backup_filepath(
    table_name: str,
    database_name: str,
    partition: str,
    timestamp: datetime,
) -> str:
    """
    Cria o caminho para salvar os dados de backup da BillingPay

    Args:
        table_name (str): Nome da tabela
        database_name (str): Nome do banco de dados
        partition (str): Partição no formato Hive
        timestamp (datetime): Timestamp de referência da execução

    Returns:
        str: Caminho para o arquivo
    """
    return str(
        Path(get_data_folder_path())
        / constants.BACKUP_BILLING_PAY_FOLDER
        / database_name
        / table_name
        / partition
        / f"{timestamp.strftime(capture_constants.FILENAME_PATTERN)}_{{n}}.json"
    )


def get_redis_last_backup(
    env: str,
    table_name: str,
    database_name: str,
    incremental_type: str,
) -> Union[int, datetime]:
    """
    Consulta no Redis o último valor capturado de uma tabela

    Args:
        env (str): prod ou dev
        table_name (str): Nome da tabela
        database_name (str): Nome do banco de dados
        incremental_type (str): Tipo de carga incremental (datetime ou integer)

    Returns:
        Union[int, datetime]: Último valor capturado
    """
    redis_key = f"{env}.backup_jae_billingpay.{database_name}.{table_name}"
    print(f"Consultando Redis: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    print(f"content = {content}")
    if incremental_type == "datetime":
        last_datetime = (
            datetime(1900, 1, 1, 0, 0, 0, tzinfo=timezone("UTC"))
            if content is None
            else datetime.strptime(
                content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY],
                treatment_constants.MATERIALIZATION_LAST_RUN_PATTERN,
            ).replace(tzinfo=timezone("UTC"))
        )
        return last_datetime.astimezone(constants.TIMEZONE)
    if incremental_type == "integer":
        last_id = (
            0 if content is None else int(content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY])
        )
        return last_id

    raise ValueError(f"Tipo {incremental_type} não encontrado.")


def get_table_data_backup_billingpay(  # noqa: PLR0913
    query: str,
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
    filepath: str,
    page_size: int,
) -> list[str]:
    """
    Captura dados de um Banco de Dados SQL fazendo paginação

    Args:
        query (str): o SELECT para ser executado
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)
        filepath (str): Modelo para criar o caminho para salvar os dados
        page_size (int): Tamanho da página para paginação

    Returns:
        list[str]: Lista de arquivos salvos
    """
    offset = 0
    base_query = f"{query} LIMIT {page_size}"
    query_paginated = f"{base_query} OFFSET 0"
    page_data_len = page_size
    current_page = 0
    filepaths = []
    while page_data_len == page_size:
        data = get_raw_db(
            query=query_paginated,
            engine=engine,
            host=host,
            user=user,
            password=password,
            database=database,
        )
        save_filepath = filepath.format(n=current_page)
        save_local_file(filepath=save_filepath, filetype="json", data=data)
        filepaths.append(save_filepath)
        page_data_len = len(data)
        print(
            f"""
            Page size: {page_size}
            Current page: {current_page}
            Current page returned {page_data_len} rows"""
        )
        current_page += 1
        offset = current_page * page_size
        query_paginated = f"{base_query} OFFSET {offset}"

    return filepaths
