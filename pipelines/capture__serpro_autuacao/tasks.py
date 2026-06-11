# -*- coding: utf-8 -*-
"""
Tasks para captura de dados do SERPRO
"""

import os
from datetime import datetime
from functools import partial

import pandas as pd
from impala.dbapi import connect
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__serpro_autuacao.constants import SERPRO_CAPTURE_PARAMS
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.fs import save_local_file


def _get_serpro_connection():
    """
    Cria conexão com o banco de dados SERPRO via Impala.

    Returns:
        Connection: Objeto de conexão com o banco
    """

    conn = connect(
        host=os.environ["radar_serpro_v2_host"],
        port=int(os.environ["radar_serpro_v2_port"]),
        user=os.environ["radar_serpro_v2_user"],
        password=os.environ["radar_serpro_v2_password"],
        auth_mechanism="LDAP",
        use_ssl=True,
        ca_cert=os.environ["radar_serpro_v2_crt"],
        database=os.environ["radar_serpro_v2_database"],
    )
    return conn


def extract_serpro_data(
    raw_filepath: str,
    timestamp: datetime,
) -> list[str]:
    """
    Extrai dados do SERPRO e salva localmente.

    Args:
        raw_filepath: Template do caminho para salvar o arquivo raw
        timestamp: Timestamp da captura para filtrar os dados

    Returns:
        list[str]: Lista com o caminho do arquivo salvo
    """
    print(f"Iniciando extração de dados do SERPRO para {timestamp.date()}")

    page_size = SERPRO_CAPTURE_PARAMS["page_size"]
    query = SERPRO_CAPTURE_PARAMS["query"].format(data=timestamp.strftime("%Y-%m-%d"))
    print(f"Executando query:\n{query}")

    filepaths = []

    with _get_serpro_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]

            page = 0
            while True:
                rows = cursor.fetchmany(page_size)
                if not rows:
                    break

                filepath = raw_filepath.format(page=page)
                df = pd.DataFrame(
                    rows, columns=columns, **SERPRO_CAPTURE_PARAMS["pre_treatment_reader_args"]
                )
                save_local_file(filepath=filepath, filetype="csv", data=df)
                filepaths.append(filepath)

                print(
                    f"""
                    Page size: {page_size}
                    Current page: {page}
                    Current page returned {len(rows)} rows"""
                )
                page += 1

    print(f"Extração concluída. Total de arquivos: {len(filepaths)}")
    return filepaths


@task(cache_policy=NO_CACHE)
def create_serpro_extractor(context: SourceCaptureContext):
    """
    Cria função de extração para o SERPRO.

    Args:
        context: Contexto da captura contendo informações do source e timestamp

    Returns:
        Callable: Função parcial configurada para extração
    """
    return partial(
        extract_serpro_data,
        raw_filepath=context.raw_filepath,
        timestamp=context.timestamp,
    )
