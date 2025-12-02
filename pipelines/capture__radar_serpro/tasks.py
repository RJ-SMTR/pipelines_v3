# -*- coding: utf-8 -*-
from datetime import datetime
from functools import partial
from typing import Union
from pathlib import Path
import os
import requests

from prefect import task

# from pipelines.capture__radar_serpro.utils import extract_serpro_data
# from utils.gcp.bigquery import SourceTable
from impala.dbapi import connect
import pandas as pd


# @task
# def create_serpro_extractor(
#     source: SourceTable, timestamp: Union[str, datetime]  # pylint: disable=W0613
# ):
#     """
#     Cria uma função para extrair dados do SERPRO

#     Args:
#         source (SourceTable): Objeto contendo informações da tabela
#         timestamp (datetime): Timestamp da execução

#     Returns:
#         Callable: Função para extração dos dados
#     """

#     return partial(extract_serpro_data, timestamp=timestamp)

@task
def setup_serpro_cert():
    #Download ssl certificate
    crt_url = os.environ['radar_serpro_v2_crt_url']
    crt_local_path = os.environ['radar_serpro_v2_crt_local_path']
    crt_res = requests.get(crt_url)
    with open(crt_local_path, "wb") as f:
        f.write(crt_res.content)
    print( f"Certificado baixado em {crt_local_path}: {Path(crt_local_path).is_file()}" )
    

@task()
def connect_to_serpro_db():
    conn = connect(
        host=os.environ['radar_serpro_v2_host'],
        port=int(os.environ['radar_serpro_v2_port']),
        user=os.environ['radar_serpro_v2_user'],
        password=os.environ['radar_serpro_v2_password'], 
        auth_mechanism="LDAP", 
        use_ssl=True, 
        ca_cert=os.environ['radar_serpro_v2_crt_local_path'], 
        database=os.environ['radar_serpro_v2_database'])
    return conn, conn.cursor()

@task(log_prints=True)
def test_serpro_connection(cursor):
    cursor.execute("SHOW SCHEMAS")
    print(cursor.fetchall())
    cursor.execute("SHOW TABLES")
    print(cursor.fetchall())
    cursor.execute(os.environ['radar_serpro_v2_test_query'])
    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print(df)