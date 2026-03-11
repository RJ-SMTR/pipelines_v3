# -*- coding: utf-8 -*-
"""Utilidades para backup incremental de dados BillingPay"""

from datetime import datetime
from pathlib import Path
from typing import Union
from zoneinfo import ZoneInfo

from prefect import runtime

from pipelines.capture__jae_backup_billingpay import constants
from pipelines.common import constants as smtr_constants
from pipelines.common.capture.default_capture import constants as capture_constants
from pipelines.common.treatment.default_treatment import (
    constants as treatment_constants,
)
from pipelines.common.utils.fs import get_data_folder_path
from pipelines.common.utils.redis import get_redis_client


def get_backup_billing_pay_flow_run_name(database_name: str) -> str:
    """
    Gera o nome da run do Flow com base no nome do banco de dados

    Args:
        database_name (str): Nome do banco de dados

    Returns:
        str: Nome formatado para a run do flow
    """
    start_time = runtime.flow_run.start_time
    return f"{database_name}: {start_time.isoformat()}"


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
    # env: str,
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
    # redis_key = f"{env}.backup_jae_billingpay.{database_name}.{table_name}"
    redis_key = f"prod.backup_jae_billingpay.{database_name}.{table_name}"

    print(f"Consultando Redis: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    print(f"content = {content}")
    if incremental_type == "datetime":
        last_datetime = (
            datetime(1900, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
            if content is None
            else datetime.strptime(
                content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY],
                treatment_constants.MATERIALIZATION_LAST_RUN_PATTERN,
            ).replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
        )
        return last_datetime
    if incremental_type == "integer":
        last_id = (
            0 if content is None else int(content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY])
        )
        return last_id

    raise ValueError(f"Tipo {incremental_type} não encontrado.")
