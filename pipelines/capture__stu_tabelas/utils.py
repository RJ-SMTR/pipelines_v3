# -*- coding: utf-8 -*-
"""
Funções auxiliares para o flow de captura de dados do STU.
"""

import json
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from pipelines.capture__stu_tabelas import constants
from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.gcp.storage import Storage


def remove_arquivos(blobs: list, date: datetime, days: int = 30) -> None:
    """
    Remove arquivos antigos do bucket.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date: Data de referência
        days: Número de dias a subtrair da data de referência
    """
    files_to_delete = []
    cutoff_date = date - timedelta(days=days)

    for blob in blobs:
        try:
            filename = blob.name.split("/")[-1]

            if not filename.endswith(".csv"):
                continue

            # Extrai a data do nome do arquivo (formato: YYYY_MM_DD_*)
            parts = filename.split("_")
            if len(parts) >= 3:  # noqa: PLR2004
                file_date = datetime.strptime(  # noqa: DTZ007
                    "_".join(parts[:3]), "%Y_%m_%d"
                )
                if file_date.date() <= cutoff_date.date():
                    files_to_delete.append(blob)

        except (ValueError, IndexError):
            continue
        except Exception as e:
            print(f"Erro ao processar arquivo {blob.name}: {str(e)}")  # noqa: RUF010
            continue

    if files_to_delete:
        for blob in files_to_delete:
            try:
                print(f"Deletando arquivo: {blob.name}")
                blob.delete()
            except Exception as e:
                print(f"Erro ao deletar arquivo {blob.name}: {str(e)}")  # noqa: RUF010
        print(f"Deletados {len(files_to_delete)} arquivos com sucesso")
    else:
        print("Nenhum arquivo antigo encontrado para deletar")


def processa_dados(blobs: list, date_str: str) -> pd.DataFrame:
    """
    Carrega e processa dados de uma data específica do bucket.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date_str: Data no formato YYYY_MM_DD

    Returns:
        pd.DataFrame: Dados processados da data especificada
    """
    files = [
        blob
        for blob in blobs
        if blob.name.split("/")[-1].startswith(date_str) and blob.name.endswith(".csv")
    ]

    if not files:
        print(f"Nenhum arquivo encontrado para {date_str}")
        return pd.DataFrame()

    print(f"Processando {len(files)} arquivos para {date_str}")

    aux_df = []
    for file in files:
        try:
            filename = file.name.split("/")[-1]
            print(f"Lendo arquivo: {filename}")

            csv_string = file.download_as_text()

            df = pd.read_csv(StringIO(csv_string), dtype=str)

            if "_airbyte_data" in df.columns:
                data = pd.json_normalize(df["_airbyte_data"].apply(json.loads))
                data = data.replace({r"[\x00-\x1F\x7F]": ""}, regex=True)
                aux_df.append(data)
            else:
                print(f"Arquivo {filename} não contém coluna _airbyte_data")

        except Exception as e:
            print(f"Erro ao processar arquivo {file.name}: {str(e)}")  # noqa: RUF010
            continue

    if not aux_df:
        return pd.DataFrame()

    result = pd.concat(aux_df, ignore_index=True)

    print(f"Total de {len(result)} registros únicos carregados para {date_str}")

    return result


def gera_hashes(blobs: list, date_str: str) -> tuple[set, list]:
    """
    Carrega os dados de uma data e os reduz a um conjunto de hashes por linha.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date_str: Data no formato YYYY_MM_DD

    Returns:
        tuple[set, list]: Conjunto de hashes por linha e lista ordenada de colunas
    """
    df = processa_dados(blobs, date_str)

    if df.empty:
        return set(), []

    colunas = sorted(df.columns)
    hashes = set(pd.util.hash_pandas_object(df[colunas], index=False))

    print(f"Gerados {len(hashes)} hashes para {date_str}")

    return hashes, colunas


def filtra_novos_registros(
    df_hoje: pd.DataFrame, hashes_anteriores: set, colunas_anteriores: list
) -> pd.DataFrame:
    """
    Retorna apenas os registros de hoje novos ou alterados em relação ao dia anterior.

    Args:
        df_hoje: DataFrame com dados de hoje
        hashes_anteriores: Conjunto de hashes por linha do dia anterior
        colunas_anteriores: Lista ordenada de colunas usada para gerar os hashes

    Returns:
        pd.DataFrame: Registros que são novos ou foram alterados
    """
    if sorted(df_hoje.columns) != colunas_anteriores:
        print("Conjunto de colunas mudou - retornando todos os registros")
        return df_hoje

    hashes_hoje = pd.util.hash_pandas_object(df_hoje[colunas_anteriores], index=False)

    return df_hoje[~hashes_hoje.isin(hashes_anteriores)]


def extract_stu_data(
    source,
    timestamp: datetime,
    raw_filepath: str,
) -> list[str]:
    """
    Extrai dados do STU a partir dos arquivos do Airbyte no GCS.

    Compara os dados de hoje com o dia anterior mais recente que tenha dados,
    salva localmente os registros novos/alterados e retorna os caminhos.

    Args:
        source: Objeto SourceTable com configurações da tabela
        timestamp: Timestamp de referência da captura
        raw_filepath: Template do caminho local com placeholder `{page}`

    Returns:
        list[str]: Lista com os caminhos dos arquivos salvos localmente
    """
    # ==== TESTE TEMPORÁRIO - REVERTER ANTES DO MERGE ====
    # Lê o ingestion de PROD mesmo rodando em dev, para validar o consumo de
    # memória das tabelas grandes. A saída (raw/source) continua em dev.
    st = Storage(
        env="prod",
        dataset_id=source.dataset_id,
        table_id=source.table_id,
        bucket_names=constants.STU_PRIVATE_BUCKET_NAMES,
    )
    # ==== FIM TESTE TEMPORÁRIO ====

    hoje = timestamp
    hoje_str = hoje.strftime("%Y_%m_%d")

    first_run = (
        source.first_timestamp is not None and timestamp.date() == source.first_timestamp.date()
    )

    print(f"Buscando dados de hoje ({hoje_str})")

    prefix = f"ingestion/source_{constants.STU_SOURCE_NAME}/stu_{source.table_id}/"
    blobs = list(st.bucket.list_blobs(prefix=prefix))

    print(f"Total de {len(blobs)} blobs encontrados no prefixo {prefix}")

    filepath = raw_filepath.format(page=0)

    # Encontra a data anterior mais recente apenas pelos nomes dos arquivos
    dates = {
        "_".join(b.name.split("/")[-1].split("_")[:3]) for b in blobs if b.name.endswith(".csv")
    }

    data_anterior_str = None
    if not first_run:
        max_days_back = 30
        for days_back in range(1, max_days_back + 1):
            aux = (hoje - timedelta(days=days_back)).strftime("%Y_%m_%d")
            if aux in dates:
                data_anterior_str = aux
                break

    # Gera os hashes do dia anterior e libera o DataFrame antes de carregar hoje
    hashes_anteriores, colunas_anteriores = set(), []
    if data_anterior_str is not None:
        print(f"Gerando hashes do dia anterior: {data_anterior_str}")
        hashes_anteriores, colunas_anteriores = gera_hashes(blobs, data_anterior_str)

    df_hoje = processa_dados(blobs, hoje_str)
    print(f"Registros de hoje: {len(df_hoje)}")

    if df_hoje.empty:
        print("Nenhum dado encontrado para hoje")
        save_local_file(filepath=filepath, filetype="csv", data=pd.DataFrame())
        return [filepath]

    if first_run or not hashes_anteriores:
        if not first_run:
            print("Sem dados no dia anterior - todos os registros são considerados novos")
        novos_registros = df_hoje
    else:
        print("Comparando dados de hoje vs dia anterior...")
        novos_registros = filtra_novos_registros(df_hoje, hashes_anteriores, colunas_anteriores)

    print(f"Total de registros novos/alterados: {len(novos_registros)}")

    # ==== TESTE TEMPORÁRIO - REVERTER ANTES DO MERGE ====
    # Delete desativado: lendo ingestion de PROD, não pode apagar arquivos de prod.
    # if not first_run:
    #     print("Iniciando limpeza de arquivos antigos...")
    #     remove_arquivos(blobs=blobs, date=hoje, days=30)
    # ==== FIM TESTE TEMPORÁRIO ====

    save_local_file(filepath=filepath, filetype="csv", data=novos_registros)
    return [filepath]
