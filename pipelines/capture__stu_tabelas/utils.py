# -*- coding: utf-8 -*-
"""
Funções auxiliares para o flow de captura de dados do STU.
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from pipelines.capture__stu_tabelas import constants
from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.gcp.storage import Storage

# Linhas por bloco ao decodificar o JSON do Airbyte
JSON_SLICE_ROWS = 50_000
HASHES_VAZIOS = pl.DataFrame(schema={"hash_chave": pl.UInt64, "hash_linha": pl.UInt64})


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


def baixa_blob(file, local_dir: str) -> str:
    """
    Baixa um blob do GCS para um diretório local.

    Baixar o arquivo inteiro antes de ler é mais rápido que ler o blob em streaming
    com `blob.open()`, que faz uma requisição por bloco.

    Args:
        file: Blob do GCS
        local_dir: Diretório local de destino

    Returns:
        str: Caminho do arquivo baixado
    """
    filepath = Path(local_dir) / file.name.split("/")[-1]
    file.download_to_filename(str(filepath))
    return str(filepath)


def is_valid_json(raw_data: pl.Series) -> bool:
    """
    Verifica se todas as linhas são JSON válido, sem considerar os tipos dos campos.

    Args:
        raw_data: Série com um JSON por linha

    Returns:
        bool: True se todas as linhas puderem ser lidas como JSON
    """
    try:
        raw_data.str.json_decode(pl.Struct({}))
    except pl.exceptions.ComputeError:
        return False
    return True


def carrega_airbyte_data(file) -> pl.DataFrame:
    """
    Carrega e normaliza a coluna _airbyte_data de um arquivo CSV do Airbyte.

    O JSON é decodificado com todos os campos como texto, para que o hash seja
    consistente entre arquivos do mesmo dia e entre dias diferentes. As colunas
    são ordenadas e os caracteres de controle removidos.

    Args:
        file: Blob do GCS contendo um CSV do Airbyte

    Returns:
        pl.DataFrame: Dados normalizados, colunas ordenadas, todas do tipo texto
    """
    filename = file.name.split("/")[-1]
    start = time.perf_counter()
    with TemporaryDirectory() as local_dir:
        filepath = baixa_blob(file=file, local_dir=local_dir)
        downloaded = time.perf_counter()
        raw = pl.read_csv(filepath, columns=["_airbyte_data"], infer_schema=False)
    raw_data = raw.get_column("_airbyte_data")

    # O JSON é decodificado em blocos para limitar a memória: o decode inteiro do
    # arquivo chega a ocupar 8x o tamanho do CSV. Cada bloco descobre seus campos e
    # depois os lê todos como texto
    fields = {}
    slices = []
    for offset in range(0, raw_data.len(), JSON_SLICE_ROWS):
        raw_slice = raw_data.slice(offset, JSON_SLICE_ROWS)
        try:
            schema = raw_slice.str.json_decode(infer_schema_length=None).dtype.to_schema()
        except pl.exceptions.ComputeError as e:
            if not is_valid_json(raw_slice):
                # JSON malformado: o arquivo é pulado, como na versão com pandas, e a
                # busca do dia anterior segue para os outros arquivos e datas
                raise ValueError(f"JSON inválido em {filename}: {e}") from e
            # Objeto/lista misturado com valores simples no mesmo campo: falha a captura
            # em vez de pular o arquivo, que perderia registros sem aviso
            raise NotImplementedError(f"JSON não suportado em {filename}: {e}") from e
        nested = [name for name, dtype in schema.items() if isinstance(dtype, (pl.Struct, pl.List))]
        if nested:
            raise NotImplementedError(f"Campos aninhados não suportados em {filename}: {nested}")
        fields.update(schema)
        slices.append(
            raw_slice.str.json_decode(pl.Struct(dict.fromkeys(schema, pl.String))).struct.unnest()
        )

    if not slices:
        print(f"{filename}: arquivo sem registros")
        return pl.DataFrame()

    data = (
        pl.concat(slices, how="diagonal")
        .select(sorted(fields))
        .with_columns(pl.all().str.replace_all(r"[\x00-\x1F\x7F]", ""))
        # Texto vazio vira nulo, como na versão com pandas, que gravava os dois iguais
        .with_columns(pl.all().replace("", None))
    )
    print(
        f"{filename}: download {downloaded - start:.1f}s, "
        f"leitura {time.perf_counter() - downloaded:.1f}s, {data.height} linhas"
    )
    return data


def gera_hashes_linhas(data: pl.DataFrame, primary_keys: list) -> pl.DataFrame:
    """
    Gera o hash da chave primária e o hash da linha inteira de cada registro.

    Args:
        data: Dados normalizados por `carrega_airbyte_data`
        primary_keys: Lista de colunas que identificam unicamente um registro

    Returns:
        pl.DataFrame: Colunas hash_chave e hash_linha, na ordem dos registros
    """
    return data.select(
        pl.struct(primary_keys).hash().alias("hash_chave"),
        pl.struct(pl.all()).hash().alias("hash_linha"),
    )


def gera_hashes(blobs: list, date_str: str, primary_keys: list) -> tuple[pl.DataFrame, list]:
    """
    Carrega os dados de uma data e gera o hash da chave e da linha de cada registro.

    Processa arquivo a arquivo, sem concatenar o dia inteiro, mantendo apenas os
    hashes em memória. Quando uma chave aparece mais de uma vez, vale a última
    ocorrência, na ordem dos arquivos.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date_str: Data no formato YYYY_MM_DD
        primary_keys: Lista de colunas que identificam unicamente um registro

    Returns:
        tuple[pl.DataFrame, list]: Hashes hash_chave/hash_linha e lista ordenada de colunas
    """
    files = [
        blob
        for blob in blobs
        if blob.name.split("/")[-1].startswith(date_str) and blob.name.endswith(".csv")
    ]

    if not files:
        print(f"Nenhum arquivo encontrado para {date_str}")
        return HASHES_VAZIOS, []

    hashes = []
    colunas = None
    for file in files:
        try:
            filename = file.name.split("/")[-1]
            print(f"Gerando hashes do arquivo: {filename}")

            data = carrega_airbyte_data(file)
            if data.is_empty():
                continue

            if colunas is None:
                colunas = data.columns
                if set(primary_keys) - set(colunas):
                    print(f"Chaves primárias ausentes em {date_str}: ignorando dia anterior")
                    return HASHES_VAZIOS, []
            elif data.columns != colunas:
                print(f"Colunas divergentes entre arquivos de {date_str}: ignorando dia anterior")
                return HASHES_VAZIOS, []

            hashes.append(gera_hashes_linhas(data=data, primary_keys=primary_keys))

        except (pl.exceptions.PolarsError, ValueError, KeyError, OSError) as e:
            print(f"Erro ao gerar hashes do arquivo {file.name}: {str(e)}")  # noqa: RUF010
            continue

    if not hashes:
        return HASHES_VAZIOS, []

    hashes_por_chave = pl.concat(hashes).unique(subset="hash_chave", keep="last")
    print(f"Gerados {hashes_por_chave.height} hashes para {date_str}")

    return hashes_por_chave, colunas or []


def filtra_novos_registros(
    df_hoje: pl.DataFrame,
    hashes_anteriores: pl.DataFrame,
    colunas_anteriores: list,
    primary_keys: list,
) -> pl.DataFrame:
    """
    Retorna apenas os registros de hoje novos ou alterados em relação ao dia anterior.

    Um registro é novo/alterado quando sua chave primária não existia no dia anterior
    ou quando o hash da linha mudou.

    Args:
        df_hoje: DataFrame com dados de hoje
        hashes_anteriores: Hashes hash_chave/hash_linha do dia anterior, uma linha por chave
        colunas_anteriores: Lista ordenada de colunas usada para gerar os hashes
        primary_keys: Lista de colunas que identificam unicamente um registro

    Returns:
        pl.DataFrame: Registros que são novos ou foram alterados
    """
    if df_hoje.columns != colunas_anteriores:
        print("Conjunto de colunas mudou - retornando todos os registros")
        return df_hoje

    return (
        pl.concat(
            [df_hoje, gera_hashes_linhas(data=df_hoje, primary_keys=primary_keys)],
            how="horizontal",
        )
        .join(
            hashes_anteriores,
            on=["hash_chave", "hash_linha"],
            how="anti",
            maintain_order="left",
        )
        .drop("hash_chave", "hash_linha")
    )


def processa_arquivos_hoje(
    files_hoje: list,
    filepath: str,
    hashes_anteriores: pl.DataFrame,
    colunas_anteriores: list,
    primary_keys: list,
) -> tuple[int, int]:
    """
    Processa e salva os registros de hoje incrementalmente, arquivo a arquivo.

    Args:
        files_hoje: Lista de blobs CSV da data atual
        filepath: Caminho local de saída
        hashes_anteriores: Hashes hash_chave/hash_linha do dia anterior
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
            if df_hoje.is_empty():
                continue
            total_registros += df_hoje.height
            colunas_hoje = df_hoje.columns

            if hashes_anteriores.is_empty():
                novos_registros = df_hoje
            else:
                novos_registros = filtra_novos_registros(
                    df_hoje,
                    hashes_anteriores,
                    colunas_anteriores,
                    primary_keys,
                )

            if novos_registros.is_empty():
                continue

            save_local_file(
                filepath=filepath,
                filetype="csv",
                data=novos_registros,
                csv_mode="a" if wrote_file else "w",
            )
            wrote_file = True
            total_novos_registros += novos_registros.height

        except (pl.exceptions.PolarsError, ValueError, KeyError, OSError) as e:
            print(f"Erro ao processar arquivo de hoje {file.name}: {str(e)}")  # noqa: RUF010
            continue

    if not wrote_file:
        save_local_file(
            filepath=filepath,
            filetype="csv",
            data=pl.DataFrame(schema=dict.fromkeys(colunas_hoje, pl.String)),
        )

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
    hashes_anteriores, colunas_anteriores = HASHES_VAZIOS, []
    if not first_run:
        max_days_back = 30
        for days_back in range(1, max_days_back + 1):
            aux = (hoje - timedelta(days=days_back)).strftime("%Y_%m_%d")
            if aux not in dates:
                continue

            print(f"Gerando hashes do dia anterior: {aux}")
            hashes_anteriores, colunas_anteriores = gera_hashes(blobs, aux, source.primary_keys)
            if not hashes_anteriores.is_empty():
                break

    files_hoje = [
        blob
        for blob in blobs
        if blob.name.split("/")[-1].startswith(hoje_str) and blob.name.endswith(".csv")
    ]
    if not files_hoje:
        print("Nenhum dado encontrado para hoje")
        save_local_file(filepath=filepath, filetype="csv", data=pl.DataFrame())
        return [filepath]

    if first_run or hashes_anteriores.is_empty():
        if not first_run:
            print("Sem dados no dia anterior - todos os registros são considerados novos")
    else:
        print("Comparando dados de hoje vs dia anterior...")

    total_registros, total_novos_registros = processa_arquivos_hoje(
        files_hoje=files_hoje,
        filepath=filepath,
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
