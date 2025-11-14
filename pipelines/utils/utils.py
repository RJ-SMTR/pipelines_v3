# -*- coding: utf-8 -*-
"""General purpose functions"""

import io
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pandas_gbq
import pendulum
import pytz
from croniter import croniter
from pandas_gbq.exceptions import GenericGBQException
from pytz import timezone

import pipelines
from pipelines import constants


def is_running_locally() -> bool:
    root_path = Path(pipelines.__file__).parent.parent

    return not str(root_path).endswith("site-packages")


def custom_serialization(obj: Any) -> Any:
    """
    Função para serializar objetos não serializaveis
    pela função json.dump

    Args:
        obj (Any): Objeto a ser serializado

    Returns:
        Any: Object serializado
    """
    if isinstance(obj, (pd.Timestamp, date)):
        if isinstance(obj, pd.Timestamp):
            if obj.tzinfo is None:
                obj = obj.tz_localize("UTC").tz_convert(constants.TIMEZONE)
        return obj.isoformat()
    elif isinstance(obj, uuid.UUID):
        return str(obj)

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def data_info_str(data: pd.DataFrame):
    """
    Retorna as informações de um Dataframe como string

    Args:
        data (pd.DataFrame): Dataframe para extrair as informações

    Returns:
        str: retorno do método data.info()
    """
    buffer = io.StringIO()
    data.info(buf=buffer)
    return buffer.getvalue()


def isostr_to_datetime(datetime_str: str) -> datetime:
    """
    Converte uma string de data no formato iso em um datetime em UTC

    Args:
        datetime_str (str): String a ser convertida

    Returns:
        datetime: String convertida em datetime
    """
    converted = datetime.fromisoformat(datetime_str)
    if converted.tzinfo is None:
        converted = converted.replace(tzinfo=pytz.UTC)
    else:
        converted = converted.astimezone(tz=pytz.timezone("UTC"))

    return converted


def create_sql_update_filter(
    env: str,
    dataset_id: str,
    table_id: str,
    primary_keys: list[str],
    content_columns_to_search: list[str],
) -> str:
    """
    Cria condição para ser usada no WHERE de queries SQL
    de modo a buscar por mudanças em um conjunto de colunas
    com base na tabela do BQ.

    Args:
        env (str): Dev ou prod.
        dataset_id (str): Dataset_id no BigQuery.
        table_id (str): Table_id no BigQuery.
        primary_keys (list[str]): Lista de primary keys da tabela.
        content_columns_to_search (list[str]): Lista de nomes das colunas
            dentro da coluna content para buscar por alterações.

    Returns:
        str: Condição para ser adicionada na query. Se a tabela não existir no BQ, retorna 1=1
    """
    project = constants.PROJECT_NAME[env]
    print(f"project = {project}")
    pks_to_concat_bq = [c.split(".")[-1] for c in primary_keys]
    content_to_concat_bq = [
        f"SAFE_CAST(JSON_VALUE(content, '$.{c.split('.')[-1]}') AS STRING)"
        for c in content_columns_to_search
    ]
    concat_arg = ",'_',"

    columns_to_concat_bq = pks_to_concat_bq + content_to_concat_bq

    columns_to_search_db = primary_keys + content_columns_to_search

    try:
        query = f"""
        SELECT DISTINCT
            CONCAT("'", {concat_arg.join(columns_to_concat_bq)}, "'")
        FROM
            `{project}.{dataset_id}.{table_id}`
        """
        print(query)
        last_values = pandas_gbq.read_gbq(query, project_id=project)

        last_values = last_values.iloc[:, 0].to_list()
        last_values = ", ".join(last_values)
        update_condition = f"""CONCAT(
                {concat_arg.join(columns_to_search_db)}
            ) NOT IN ({last_values})
        """

    except GenericGBQException as err:
        if "404 Not found" in str(err):
            print("table not found, setting updates to 1=1")
            update_condition = "1=1"
        else:
            raise err

    return update_condition


def get_last_materialization_redis_key(env: str, dataset_id: str, table_id: str) -> str:
    """
    Gera o nome para ser usado na key onde serão salvos no Redis os dados
    de controle para materialização de uma tabela

    Args:
        env (str): dev ou prod
        dataset_id (str): nome do dataset no dbt
        table_id (str): nome da tabela no dbt

    Returns:
        str: nome da key
    """
    key = dataset_id + "." + table_id
    if env == "dev":
        key = f"{env}.{key}"

    return key


def cron_date_range(cron_expr: str, start_time: datetime, end_time: datetime) -> list[datetime]:
    """
    Gera um range de datetimes com base em uma expressão cron entre dois datetimes.

    Args:
        cron_expr (str): Expressão cron
        start_time (datetime): Datetime de início do range
        end_time (datetime): Datetime de fim do range

    Returns:
        list[datetime]: lista com o range de datetimes

    """
    iterator = croniter(cron_expr, start_time)
    current_date = iterator.get_next(datetime)
    datetimes = []

    while True:
        if current_date <= end_time:
            datetimes.append(current_date)
        else:
            break
        current_date = iterator.get_next(datetime)

    return datetimes


def cron_get_last_date(cron_expr: str, timestamp: datetime) -> datetime:
    """
    Com base em uma expressão cron, retorna a última data até um datetime de referência

    Args:
        cron_expr (str): Expressão cron
        timestamp (datetime): datetime de referência

    Returns:
        datetime: última data do cron
    """
    return croniter(cron_expr, timestamp).get_prev(datetime)


def cron_get_next_date(cron_expr: str, timestamp: datetime) -> datetime:
    """
    Com base em uma expressão cron, retorna a próxima data a partir de um datetime de referência

    Args:
        cron_expr (str): Expressão cron
        timestamp (datetime): datetime de referência

    Returns:
        datetime: próxima data do cron
    """
    return croniter(cron_expr, timestamp).get_next(datetime)


def convert_timezone(timestamp: datetime) -> datetime:
    """
    Converte um datetime para a timezone padrão definida nas constantes

    Args:
        timestamp (datetime): Datetime a ser convertido

    Returns:
        datetime: Datetime com informação de timezone
    """
    tz = timezone(constants.TIMEZONE)

    if isinstance(timestamp, pendulum.DateTime):
        pendulum_tz = timestamp.timezone
        timestamp = datetime.fromtimestamp(timestamp.timestamp(), tz=pendulum_tz)

    if timestamp.tzinfo is None:
        timestamp = tz.localize(timestamp)
    else:
        timestamp = timestamp.astimezone(tz=tz)

    return timestamp
