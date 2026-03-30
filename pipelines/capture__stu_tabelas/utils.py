# -*- coding: utf-8 -*-
"""
Funções auxiliares para o flow de captura de dados do STU.
"""

import json
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from pipelines.capture__stu_tabelas import constants
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


def compara_dataframes(df_hoje: pd.DataFrame, df_ontem: pd.DataFrame) -> pd.DataFrame:
    """
    Compara dois DataFrames e retorna apenas registros novos ou alterados.

    Args:
        df_hoje: DataFrame com dados de hoje
        df_ontem: DataFrame com dados de ontem

    Returns:
        pd.DataFrame: Registros que são novos ou foram alterados
    """
    new_columns = set(df_hoje.columns) - set(df_ontem.columns)

    if new_columns:
        print(f"Novas colunas detectadas: {new_columns} - retornando todos os registros")
        return df_hoje

    common_columns = [col for col in df_hoje.columns if col in df_ontem.columns]
    if not common_columns:
        print("Nenhuma coluna em comum entre hoje e ontem - retornando todos os registros")
        return df_hoje

    merged = df_hoje.merge(df_ontem, how="left", indicator=True)

    new_records = merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)

    return new_records


def extract_stu_data(source, timestamp: datetime) -> pd.DataFrame:
    """
    Extrai dados do STU a partir dos arquivos do Airbyte no GCS.

    Compara os dados de hoje com o dia anterior mais recente que tenha dados
    e retorna apenas os registros que são novos ou foram alterados.

    Args:
        source: Objeto SourceTable com configurações da tabela
        timestamp: Timestamp de referência da captura

    Returns:
        pd.DataFrame: DataFrame com os registros novos/alterados
    """
    st = Storage(
        env=source.env,
        dataset_id=source.dataset_id,
        table_id=source.table_id,
        bucket_names=constants.STU_PRIVATE_BUCKET_NAMES,
    )

    hoje = timestamp
    hoje_str = hoje.strftime("%Y_%m_%d")

    first_run = (
        source.first_timestamp is not None and timestamp.date() == source.first_timestamp.date()
    )

    print(f"Buscando dados de hoje ({hoje_str})")

    prefix = f"ingestion/source_{constants.STU_SOURCE_NAME}/stu_{source.table_id}/"
    blobs = list(st.bucket.list_blobs(prefix=prefix))

    print(f"Total de {len(blobs)} blobs encontrados no prefixo {prefix}")

    df_hoje = processa_dados(blobs, hoje_str)
    print(f"Registros de hoje: {len(df_hoje)}")

    if df_hoje.empty:
        print("Nenhum dado encontrado para hoje")
        return pd.DataFrame()

    if first_run:
        return df_hoje

    max_days_back = 30
    df_anterior = pd.DataFrame()

    dates = {
        "_".join(b.name.split("/")[-1].split("_")[:3]) for b in blobs if b.name.endswith(".csv")
    }

    for days_back in range(1, max_days_back + 1):
        data_anterior = hoje - timedelta(days=days_back)
        data_anterior_str = data_anterior.strftime("%Y_%m_%d")

        if data_anterior_str not in dates:
            continue

        print(f"Buscando dados de {data_anterior_str} ({days_back} dia(s) atrás)")
        df_anterior = processa_dados(blobs, data_anterior_str)

        if not df_anterior.empty:
            print(f"Encontrados {len(df_anterior)} registros em {data_anterior_str}")
            break

    if df_anterior.empty:
        print(
            f"Sem dados nos últimos {max_days_back} dias - "
            "todos os registros são considerados novos"
        )
        new_records = df_hoje
    else:
        print("Comparando dados de hoje vs dia anterior encontrado...")
        new_records = compara_dataframes(df_hoje, df_anterior)

    print(f"Total de registros novos/alterados: {len(new_records)}")

    print("Iniciando limpeza de arquivos antigos...")
    remove_arquivos(blobs=blobs, date=hoje, days=30)

    return new_records
