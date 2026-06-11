# -*- coding: utf-8 -*-
"""
Funções auxiliares para o flow de captura de dados do STU.
"""

import json
from datetime import datetime, timedelta

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


def carrega_airbyte_data(file) -> pd.DataFrame:
    """
    Carrega e normaliza a coluna _airbyte_data de um arquivo CSV do Airbyte.

    As colunas são ordenadas e convertidas para `string[pyarrow]`, que é compacto
    (sem overhead de objeto Python por célula) e consistente para hashing, tanto
    entre arquivos do mesmo dia quanto entre dias diferentes.

    Args:
        file: Blob do GCS contendo um CSV do Airbyte

    Returns:
        pd.DataFrame: Dados normalizados, colunas ordenadas, dtype string[pyarrow]
    """
    with file.open("rt", encoding="utf-8") as csv_file:
        df = pd.read_csv(csv_file, dtype=str, usecols=["_airbyte_data"])
    data = pd.json_normalize(
        df["_airbyte_data"].apply(lambda raw: json.loads(raw, parse_int=str, parse_float=str))
    )

    for column in data.select_dtypes(include="object").columns:
        if data[column].isna().all():
            continue
        data[column] = data[column].replace({r"[\x00-\x1F\x7F]": ""}, regex=True)

    return data[sorted(data.columns)].astype("string[pyarrow]")


def gera_hashes(blobs: list, date_str: str, primary_keys: list) -> tuple[dict, list]:
    """
    Carrega os dados de uma data e cria um mapa de hash da chave para hash da linha.

    Processa arquivo a arquivo, sem concatenar o dia inteiro, mantendo apenas o
    dicionário de hashes em memória.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date_str: Data no formato YYYY_MM_DD
        primary_keys: Lista de colunas que identificam unicamente um registro

    Returns:
        tuple[dict, list]: Mapa hash_pk -> hash_linha e lista ordenada de colunas
    """
    files = [
        blob
        for blob in blobs
        if blob.name.split("/")[-1].startswith(date_str) and blob.name.endswith(".csv")
    ]

    if not files:
        print(f"Nenhum arquivo encontrado para {date_str}")
        return {}, []

    hashes_por_chave = {}
    colunas = None
    for file in files:
        try:
            filename = file.name.split("/")[-1]
            print(f"Gerando hashes do arquivo: {filename}")

            data = carrega_airbyte_data(file)

            if colunas is None:
                colunas = list(data.columns)
                if set(primary_keys) - set(colunas):
                    print(f"Chaves primárias ausentes em {date_str}: ignorando dia anterior")
                    return {}, []
            elif list(data.columns) != colunas:
                print(f"Colunas divergentes entre arquivos de {date_str}: ignorando dia anterior")
                return {}, []

            hashes_chave = pd.util.hash_pandas_object(data[primary_keys], index=False)
            hashes_linha = pd.util.hash_pandas_object(data, index=False)
            hashes_por_chave.update(zip(hashes_chave, hashes_linha, strict=True))

        except Exception as e:
            print(f"Erro ao gerar hashes do arquivo {file.name}: {str(e)}")  # noqa: RUF010
            continue

    print(f"Gerados {len(hashes_por_chave)} hashes para {date_str}")

    return hashes_por_chave, colunas or []


def filtra_novos_registros(
    df_hoje: pd.DataFrame,
    hashes_anteriores: dict,
    colunas_anteriores: list,
    primary_keys: list,
) -> pd.DataFrame:
    """
    Retorna apenas os registros de hoje novos ou alterados em relação ao dia anterior.

    Um registro é novo/alterado quando sua chave primária não existia no dia anterior
    ou quando o hash da linha mudou.

    Args:
        df_hoje: DataFrame com dados de hoje
        hashes_anteriores: Mapa hash_pk -> hash_linha do dia anterior
        colunas_anteriores: Lista ordenada de colunas usada para gerar os hashes
        primary_keys: Lista de colunas que identificam unicamente um registro

    Returns:
        pd.DataFrame: Registros que são novos ou foram alterados
    """
    if list(df_hoje.columns) != colunas_anteriores:
        print("Conjunto de colunas mudou - retornando todos os registros")
        return df_hoje

    hashes_chave = pd.util.hash_pandas_object(df_hoje[primary_keys], index=False)
    hashes_linha = pd.util.hash_pandas_object(df_hoje, index=False)
    novos_ou_alterados = [
        hashes_anteriores.get(chave) != linha
        for chave, linha in zip(hashes_chave, hashes_linha, strict=True)
    ]

    return df_hoje[novos_ou_alterados]


def processa_arquivos_hoje(  # noqa: PLR0913
    files_hoje: list,
    filepath: str,
    first_run: bool,
    hashes_anteriores: dict,
    colunas_anteriores: list,
    primary_keys: list,
) -> tuple[int, int]:
    """
    Processa e salva os registros de hoje incrementalmente, arquivo a arquivo.

    Args:
        files_hoje: Lista de blobs CSV da data atual
        filepath: Caminho local de saída
        first_run: Indica se é a primeira captura da tabela
        hashes_anteriores: Mapa hash_pk -> hash_linha do dia anterior
        colunas_anteriores: Lista ordenada de colunas do dia anterior
        primary_keys: Lista de colunas que identificam unicamente um registro

    Returns:
        tuple[int, int]: Total de registros lidos e total de registros novos/alterados
    """
    total_registros = 0
    total_novos_registros = 0
    wrote_file = False
    colunas_hoje = []

    for file in files_hoje:
        try:
            filename = file.name.split("/")[-1]
            print(f"Lendo arquivo de hoje: {filename}")

            df_hoje = carrega_airbyte_data(file)
            total_registros += len(df_hoje)
            colunas_hoje = list(df_hoje.columns)

            if first_run or not hashes_anteriores:
                novos_registros = df_hoje
            else:
                novos_registros = filtra_novos_registros(
                    df_hoje,
                    hashes_anteriores,
                    colunas_anteriores,
                    primary_keys,
                )

            if novos_registros.empty:
                continue

            save_local_file(
                filepath=filepath,
                filetype="csv",
                data=novos_registros,
                csv_mode="a" if wrote_file else "w",
            )
            wrote_file = True
            total_novos_registros += len(novos_registros)

        except Exception as e:
            print(f"Erro ao processar arquivo de hoje {file.name}: {str(e)}")  # noqa: RUF010
            continue

    if not wrote_file:
        save_local_file(filepath=filepath, filetype="csv", data=pd.DataFrame(columns=colunas_hoje))

    return total_registros, total_novos_registros


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

    filepath = raw_filepath.format(page=0)

    # Datas disponíveis nos nomes dos arquivos do bucket
    dates = {
        "_".join(b.name.split("/")[-1].split("_")[:3]) for b in blobs if b.name.endswith(".csv")
    }

    # Procura o dia anterior mais recente com dados válidos e gera seus hashes.
    # Dias vazios/corrompidos são pulados, continuando a busca mais para trás.
    hashes_anteriores, colunas_anteriores = {}, []
    if not first_run:
        max_days_back = 30
        for days_back in range(1, max_days_back + 1):
            aux = (hoje - timedelta(days=days_back)).strftime("%Y_%m_%d")
            if aux not in dates:
                continue

            print(f"Gerando hashes do dia anterior: {aux}")
            hashes_anteriores, colunas_anteriores = gera_hashes(blobs, aux, source.primary_keys)
            if hashes_anteriores:
                break

    files_hoje = [
        blob
        for blob in blobs
        if blob.name.split("/")[-1].startswith(hoje_str) and blob.name.endswith(".csv")
    ]
    if not files_hoje:
        print("Nenhum dado encontrado para hoje")
        save_local_file(filepath=filepath, filetype="csv", data=pd.DataFrame())
        return [filepath]

    if first_run or not hashes_anteriores:
        if not first_run:
            print("Sem dados no dia anterior - todos os registros são considerados novos")
    else:
        print("Comparando dados de hoje vs dia anterior...")

    total_registros, total_novos_registros = processa_arquivos_hoje(
        files_hoje=files_hoje,
        filepath=filepath,
        first_run=first_run,
        hashes_anteriores=hashes_anteriores,
        colunas_anteriores=colunas_anteriores,
        primary_keys=source.primary_keys,
    )

    print(f"Registros de hoje: {total_registros}")
    print(f"Total de registros novos/alterados: {total_novos_registros}")

    if not first_run:
        print("Iniciando limpeza de arquivos antigos...")
        remove_arquivos(blobs=blobs, date=hoje, days=30)

    return [filepath]
