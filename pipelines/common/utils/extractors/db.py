# -*- coding: utf-8 -*-
"""Module to get data from databases"""

import pandas as pd
from sqlalchemy import create_engine

from pipelines.common.utils.database import create_database_url
from pipelines.common.utils.fs import save_local_file


def get_raw_db(  # noqa: PLR0913
    query: str,
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
    raw_filepath: str,
    max_retries: int = 10,
) -> list[str]:
    """
    Captura dados de um Banco de Dados SQL

    Args:
        query (str): o SELECT para ser executado
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)
        raw_filepath (str): Caminho para salvar os arquivos
        max_retries (int): Quantidades de retries para efetuar a query

    Returns:
        list[str]: Lista com o caminho onde os dados foram salvos
    """

    url = create_database_url(
        engine=engine,
        host=host,
        user=user,
        password=password,
        database=database,
    )
    connection = create_engine(url)
    for retry in range(1, max_retries + 1):
        try:
            print(f"[ATTEMPT {retry}/{max_retries}]: {query}")
            data = pd.read_sql(sql=query, con=connection)
            data = data.to_dict(orient="records")
            for d in data:
                for k, v in d.items():
                    if pd.isna(v):
                        d[k] = None
            break
        except Exception as err:
            if retry == max_retries:
                raise err

    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype="json", data=data)

    return [filepath]


def get_raw_db_paginated(  # noqa: PLR0913
    query: str,
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
    page_size: int,
    raw_filepath: str,
    max_retries: int = 10,
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
        page_size (int): Número máximo de registros em uma página
        raw_filepath (int): Caminho para salvar os arquivos
        max_retries (int): Quantidades de retries para efetuar a query
    Returns:
        list[str]: Lista com os caminhos onde os dados foram salvos
    """
    offset = 0
    base_query = f"{query} LIMIT {page_size}"
    query = f"{base_query} OFFSET 0"
    page_data_len = page_size
    current_page = 0
    filepaths = []
    while page_data_len == page_size:
        page_data = get_raw_db(
            query=query,
            engine=engine,
            host=host,
            user=user,
            password=password,
            database=database,
            max_retries=max_retries,
        )
        filepath = raw_filepath.format(page=current_page)
        save_local_file(filepath=filepath, filetype="json", data=page_data)
        filepaths.append(filepath)
        page_data_len = len(page_data)
        print(
            f"""
            Page size: {page_size}
            Current page: {current_page}
            Current page returned {page_data_len} rows"""
        )
        current_page += 1
        offset = current_page * page_size
        query = f"{base_query} OFFSET {offset}"

    return filepaths
