# -*- coding: utf-8 -*-
"""Tasks para backup incremental de dados BillingPay da Jaé"""

from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import task
from pytz import timezone
from sqlalchemy import DATE, DATETIME, TIMESTAMP, create_engine, inspect

from pipelines.capture__jae_backup_billingpay import constants
from pipelines.capture__jae_backup_billingpay.utils import (
    create_billingpay_backup_filepath,
    get_redis_last_backup,
)
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.common.treatment.default_treatment import (
    constants as treatment_constants,
)
from pipelines.common.utils.database import (
    create_database_url,
    list_accessible_tables,
)
from pipelines.common.utils.extractors.db import get_db_data, get_raw_db_paginated
from pipelines.common.utils.fs import create_partition
from pipelines.common.utils.gcp.storage import Storage
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.secret import get_env_secret


@task
def get_jae_db_config(database_name: str) -> dict[str, str]:
    """
    Cria as configurações de conexão com o banco de dados

    Args:
        database_name (str): Nome do banco de dados

    Returns:
        dict[str, str]: Dicionário com os argumentos para a função create_database_url
    """
    secrets = get_env_secret(jae_constants.JAE_SECRET_PATH)
    settings = jae_constants.JAE_DATABASE_SETTINGS[database_name]
    return {
        "engine": settings["engine"],
        "host": settings["host"],
        "user": secrets["user"],
        "password": secrets["password"],
        "database": database_name,
    }


@task
def get_table_info(
    env: str,
    database_name: str,
    database_config: dict[str, str],
    timestamp: datetime,
    table_id: Optional[str] = None,
) -> list[dict[str, str]]:
    """
    Busca as informações de todas as tabelas disponíveis em um banco de dados

    Args:
        env (str): prod ou dev
        database_name (str): Nome do banco de dados
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        timestamp (datetime): Timestamp de referência da execução
        table_id (Optional[str]): ID específico da tabela (opcional)

    Returns:
        list[dict[str, str]]: Lista com dicionários contendo:
            - nome da tabela
            - tipo de carga incremental
            - caminho para salvar o arquivo
            - valor salvo no redis (se houver)
            - partição do arquivo
    """
    database_url = create_database_url(**database_config)
    engine = create_engine(database_url)
    inspector = inspect(engine)
    tables_config = constants.BACKUP_JAE_BILLING_PAY[database_name]
    partition = create_partition(timestamp=timestamp, partition_date_only=True)

    if table_id is not None:
        table_names = [table_id]
    else:
        table_names = [
            t
            for t in list_accessible_tables(engine=engine)
            if t not in tables_config.get("exclude", [])
            and isinstance(tables_config.get("filter", {}).get(t, []), list)
        ]

    custom_select = tables_config.get("custom_select", {})
    filtered_tables = tables_config.get("filter", [])
    result = [
        {
            "table_name": t,
            "incremental_type": None,
            "filepath": create_billingpay_backup_filepath(
                table_name=t,
                database_name=database_name,
                partition=partition,
                timestamp=timestamp,
            ),
            "partition": partition,
            "custom_select": custom_select.get(t),
        }
        for t in table_names
        if t not in filtered_tables
    ]

    for table in [t for t in table_names if t in filtered_tables]:
        filter_columns = filtered_tables[table]

        if filter_columns[0] == "count(*)":
            with engine.connect() as conn:
                current_count = pd.read_sql(f"select count(*) as ct from {table}", conn).to_dict(
                    orient="records"
                )[0]["ct"]
                last_count = get_redis_last_backup(
                    env=env,
                    table_name=table,
                    database_name=database_name,
                    incremental_type="integer",
                )

                if current_count != last_count:
                    result.append(
                        {
                            "table_name": table,
                            "incremental_type": "count",
                            "filepath": create_billingpay_backup_filepath(
                                table_name=table,
                                database_name=database_name,
                                partition=partition,
                                timestamp=timestamp,
                            ),
                            "partition": partition,
                            "custom_select": custom_select.get(table),
                            "redis_save_value": current_count,
                        }
                    )
            continue

        column_types = [
            c["type"]
            for c in inspector.get_columns(table_name=table)
            if c["name"] in filter_columns
        ]
        has_date_type = len(column_types) > 0 and isinstance(
            next(iter(column_types), None), (TIMESTAMP, DATE, DATETIME)
        )

        if len(filter_columns) > 1 or table in custom_select.keys() or has_date_type:
            incremental_type = "datetime"
        else:
            incremental_type = "integer"

        result.append(
            {
                "table_name": table,
                "incremental_type": incremental_type,
                "filepath": create_billingpay_backup_filepath(
                    table_name=table,
                    database_name=database_name,
                    partition=partition,
                    timestamp=timestamp,
                ),
                "last_capture": get_redis_last_backup(
                    env=env,
                    table_name=table,
                    database_name=database_name,
                    incremental_type=incremental_type,
                ),
                "partition": partition,
                "custom_select": custom_select.get(table),
            }
        )

    return result


@task
def get_non_filtered_tables(
    database_name: str,
    database_config: dict[str, str],
    table_info: list[dict[str, str]],
) -> tuple[bool, list[dict]]:
    """
    Busca tabelas com mais de 5000 linhas que não estejam com filtro configurado

    Args:
        database_name (str): Nome do banco de dados
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        table_info (list[dict[str, str]]): Lista com as informações das tabelas

    Returns:
        tuple[bool, list[dict]]: Tupla contendo:
            - bool: Se deve notificar o discord ou não
            - list[dict]: Dicionário com as tabelas com mais de 5000 registros
    """
    tables_config = constants.BACKUP_JAE_BILLING_PAY[database_name]
    no_filter_tables = [
        t["table_name"]
        for t in table_info
        if t["table_name"] not in tables_config.get("filter", [])
    ]
    if len(no_filter_tables) == 0:
        return False, []
    database_url = create_database_url(**database_config)
    engine = create_engine(database_url)
    result = []
    with engine.connect() as conn:
        for table in no_filter_tables:
            print(table)
            df = pd.read_sql(f"select count(*) as ct from {table}", conn)
            df["table"] = table
            result.append(df)
    df_final = pd.concat(result)
    tables = (
        df_final.loc[df_final["ct"] > constants.MAX_UNFILTERED_TABLE_ROWS]
        .sort_values("ct", ascending=False)
        .to_dict(orient="records")
    )

    return len(tables) > 0, tables


@task
def create_non_filtered_discord_message(database_name: str, table_count: list[dict]) -> str:
    """
    Cria a mensagem para ser enviada no discord caso haja tabelas grandes sem filtro

    Args:
        database_name (str): Nome do banco de dados
        table_count (list[dict]): Dicionário com as tabelas e a contagem de registros

    Returns:
        str: Mensagem para ser enviada no discord
    """
    message = f"""
Database: {database_name}
As seguintes tabelas não possuem filtros:
"""
    message += "\n"
    message += "\n".join([f"{t['table']}: {t['ct']} registros" for t in table_count])
    return message


@task
def get_raw_backup_billingpay(
    table_info: list[dict[str, str]],
    database_config: dict[str, str],
    end_timestamp: datetime,
) -> list[dict[str, str]]:
    """
    Captura os dados das tabelas do banco informado

    Args:
        table_info (list[dict[str, str]]): Lista com as informações das tabelas
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        end_timestamp (datetime) Timestamp final da captura

    Returns:
        list[dict[str, str]]: Lista com as informações das tabelas atualizada
    """
    new_table_info = []
    for table in table_info:
        table_name = table["table_name"]
        if table["custom_select"] is not None:
            sql = table["custom_select"]
        else:
            sql = f"SELECT * FROM {table_name}"

        if "{filter}" not in sql:
            sql += " WHERE {filter}"

        where = "1=1"
        if table["incremental_type"] == "datetime":
            timestamp_str = (
                table.get(
                    "last_value",
                    end_timestamp,
                )
                .astimezone(tz=timezone("UTC"))
                .strftime("%Y-%m-%d %H:%M:%S")
            )
            last_capture_str = (
                table["last_capture"].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
            )
            if "{start}" in sql:
                sql = sql.replace("{start}", last_capture_str)
            if "{end}" in sql:
                sql = sql.replace("{end}", timestamp_str)

            where = " OR ".join(
                [
                    f"({c} >= '{last_capture_str}' AND {c} < '{timestamp_str}')"
                    for c in constants.BACKUP_JAE_BILLING_PAY[database_config["database"]][
                        "filter"
                    ][table_name]
                ]
            )
        elif table["incremental_type"] == "integer":
            id_column = constants.BACKUP_JAE_BILLING_PAY[database_config["database"]]["filter"][
                table_name
            ][0]
            max_id = get_db_data(
                f"select max({id_column}) as max_id FROM {table_name}",
                **database_config,
            )[0]["max_id"]
            where = f"{id_column} BETWEEN {table['last_capture']} AND {max_id}"
            table["redis_save_value"] = max_id
        sql = sql.format(filter=where)

        filepath = get_raw_db_paginated(
            query=sql,
            raw_filepath=table["filepath"],
            page_size=constants.BACKUP_JAE_BILLING_PAY[database_config["database"]]
            .get("page_size", {})
            .get(table_name, 200_000),
            **database_config,
        )
        table["filepath"] = filepath
        new_table_info.append(table)
    return new_table_info


@task
def upload_backup_billingpay(
    env: str, table_info: list[dict[str, str]], database_name: str
) -> list[dict[str, str]]:
    """
    Sobe os dados do backup para o storage

    Args:
        env (str): prod ou dev
        table_info (list[dict[str, str]]): Lista de dicionários com informações das tabelas
        database_name (str): Nome do banco de dados

    Returns:
        list[dict]: Lista de dicionários com informações das tabelas
    """
    for table in table_info:
        for filepath in table["filepath"]:
            Storage(env=env, dataset_id=database_name, table_id=table["table_name"]).upload_file(
                mode=constants.BACKUP_BILLING_PAY_FOLDER,
                filepath=filepath,
                partition=table["partition"],
            )

    return table_info


@task
def set_redis_backup_billingpay(
    env: str,
    table_info: list[dict[str, str]],
    database_name: str,
    end_timestamp: datetime,
):
    """
    Atualiza o Redis com os novos dados capturados

    Args:
        env (str): prod ou dev
        table_info (list[dict[str, str]]): Lista de dicionários com as informações das tabelas
        database_name (str): Nome do banco de dados
        end_timestamp (datetime): Timestamp final da captura
    """
    for table in table_info:
        if table["incremental_type"] is None:
            continue
        redis_key = f"{env}.backup_jae_billingpay.{database_name}.{table['table_name']}"
        redis_client = get_redis_client()
        content = redis_client.get(redis_key)
        if table["incremental_type"] == "datetime":
            save_value = end_timestamp.strftime(
                treatment_constants.MATERIALIZATION_LAST_RUN_PATTERN
            )
        else:
            save_value = table["redis_save_value"]

        if not content:
            print(f"Saving value: {save_value} on key {redis_key}")
            content = {constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY: save_value}
            redis_client.set(redis_key, content)
        elif (
            content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY] < save_value
            or table["incremental_type"] == "count"
        ):
            print(f"Saving value: {save_value} on key {redis_key}")
            content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY] = save_value
            redis_client.set(redis_key, content)
        else:
            print(f"[{redis_key}] {save_value} é menor que o valor salvo no Redis")
