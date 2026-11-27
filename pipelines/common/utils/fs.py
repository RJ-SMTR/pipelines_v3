# -*- coding: utf-8 -*-
"""Module to deal with the filesystem"""

import json
import os
from collections.abc import Iterator
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl

from pipelines import common
from pipelines.common.utils.utils import custom_serialization, is_running_locally


def get_project_root_path() -> Path:
    """
    Retorna o caminho da raiz do projeto.
    """
    return Path(files(common)).parent.parent


def get_root_path() -> Path:
    """
    Retorna o caminho raiz.
    """
    if is_running_locally():
        return get_project_root_path()
    else:
        return Path("/app")


def get_data_folder_path() -> str:
    """
    Retorna a pasta raíz para salvar os dados

    Returns:
        str: Caminho para a pasta data
    """
    root = get_root_path()
    return str(root / os.getenv("DATA_FOLDER", "data"))


def get_filetype(filepath: str):
    """Retorna a extensão de um arquivo

    Args:
        filepath (str): caminho para o arquivo
    """
    return Path(filepath).suffix.removeprefix(".")


def save_local_file(
    filepath: str,
    filetype: str,
    data: str | dict | list[dict] | pd.DataFrame | pl.DataFrame,
    csv_mode="w",
):
    """
    Salva um arquivo localmente

    Args:
        filepath (str): Caminho para salvar o arquivo
        filetype (str): Extensão do arquivo
        data (str | dict | list[dict] | pd.DataFrame | pl.DataFrame): Dados que serão salvos
            no arquivo
        csv_mode (str): Modo de escrita no arquivo CSV
    """
    print(f"Saving data on local file: {filepath}")

    print("Creating parent folder...")
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    print("Parent folder created!")

    if isinstance(data, pl.DataFrame):
        print("Received a Polars DataFrame, saving file as CSV")
        with Path(filepath).open("ab" if csv_mode == "a" else "wb") as file:
            data.write_csv(file, include_header=csv_mode != "a")

    elif isinstance(data, pd.DataFrame):
        print("Received a DataFrame, saving file as CSV")
        if csv_mode == "a":
            data.to_csv(filepath, index=False, header=False, mode="a")
        else:
            data.to_csv(filepath, index=False)

    else:
        print(f"Saving {filetype.upper()}")
        with Path(filepath).open("w", encoding="utf-8") as file:
            if filetype == "json":
                if isinstance(data, str):
                    print("Converting string to python object")
                    data = json.loads(data)

                json.dump(data, file, default=custom_serialization)

            elif filetype in ("txt", "csv"):
                file.write(data)

            else:
                raise NotImplementedError(
                    "Unsupported raw file extension. Supported only: json, csv and txt"
                )

    print("File saved!")


RAW_READ_BATCH_SIZE = 50_000

# Argumentos herdados do pandas (pretreatment_reader_args) que só definiam tipos.
# Com Polars os valores são lidos como texto (CSV) ou com o tipo do próprio JSON,
# então esses argumentos não têm mais efeito.
RAW_READER_IGNORED_ARGS = ("dtype", "convert_dates")


def get_csv_reader_args(reader_args: dict) -> dict:
    """
    Traduz os argumentos de leitura de CSV do pandas para o pl.scan_csv

    Args:
        reader_args (dict): Argumentos no formato do pd.read_csv

    Returns:
        dict: Argumentos no formato do pl.scan_csv
    """
    csv_args = {"infer_schema": False}
    for key, value in reader_args.items():
        if key in RAW_READER_IGNORED_ARGS or key == "chunksize":
            continue
        if key in ("sep", "delimiter"):
            csv_args["separator"] = value
        elif key == "names":
            csv_args["new_columns"] = list(value)
        elif key == "header" and value in (None, 0):
            csv_args["has_header"] = value == 0
        elif key == "skiprows":
            csv_args["skip_rows"] = value
        elif key == "encoding":
            csv_args["encoding"] = value
        elif key == "index_col" and value is False:
            continue
        else:
            raise ValueError(f"Argumento de leitura não suportado para CSV: {key}={value!r}")
    # Como no pandas, `names` sem `header=0` indica arquivo sem cabeçalho
    if "new_columns" in csv_args:
        csv_args.setdefault("has_header", False)
    return csv_args


def read_raw_data(filepath: str, reader_args: Optional[dict] = None) -> Iterator[pl.DataFrame]:
    """
    Lê os dados de um arquivo Raw em blocos

    Args:
        filepath (str): Caminho do arquivo
        reader_args (dict, optional): Argumentos de leitura no formato do pandas
            (pretreatment_reader_args). `chunksize` define o tamanho dos blocos

    Returns:
        Iterator[pl.DataFrame]: Blocos de dados lidos
    """

    print(f"Reading raw data in {filepath}")
    reader_args = reader_args or {}
    batch_size = reader_args.get("chunksize") or RAW_READ_BATCH_SIZE

    filetype = get_filetype(filepath=filepath)

    print(f"Reading {filetype.upper()}")
    if filetype == "json":
        unsupported = [k for k in reader_args if k not in (*RAW_READER_IGNORED_ARGS, "chunksize")]
        if unsupported:
            raise ValueError(f"Argumentos de leitura não suportados para JSON: {unsupported}")
        data = pl.read_json(filepath, infer_schema_length=None)
        yield from data.iter_slices(n_rows=batch_size)

    elif filetype in ("txt", "csv"):
        csv_args = get_csv_reader_args(reader_args=reader_args)
        # Linhas em branco viram linhas só com nulos; o pandas as ignorava (skip_blank_lines)
        not_blank = ~pl.all_horizontal(pl.all().is_null())
        # Campos vazios entre aspas ("") viram texto vazio no Polars; o pandas os lia como nulos
        empty_to_null = pl.all().replace("", None)
        if "encoding" not in csv_args:
            data = pl.scan_csv(filepath, **csv_args).filter(not_blank).with_columns(empty_to_null)
            yield from data.collect_batches(chunk_size=batch_size)
        else:
            # scan_csv só lê UTF-8; read_csv decodifica a codificação informada (latin1 do RDO)
            data = pl.read_csv(filepath, **csv_args).filter(not_blank).with_columns(empty_to_null)
            yield from data.iter_slices(n_rows=batch_size)

    else:
        raise NotImplementedError(
            "Unsupported raw file extension. Supported only: json, csv and txt"
        )


def create_partition(
    timestamp: datetime,
    partition_date_only: bool,
    partition_key: str = "data",
    partition_value: Optional[str] = None,
) -> str:
    """
    Cria a partição Hive de acordo com a timestamp

    Args:
        timestamp (datetime): timestamp de referência
        partition_date_only (bool): True se o particionamento deve ser feito apenas por data
            False se o particionamento deve ser feito por data e hora
        partition_key (str): nome Hive da partição de data
        partition_value (Optional[str]): valor de data Hive. Se omitido, usa a data do timestamp.

    Returns:
        str: string com o particionamento
    """

    print("Criando partição...")
    print(f"Timestamp recebida: {timestamp}")

    if partition_value is None:
        partition_value = timestamp.strftime("%Y-%m-%d")
    partition = f"{partition_key}={partition_value}"
    if not partition_date_only:
        partition = f"{partition}/hora={timestamp.strftime('%H')}"

    print(f"Partição criada com sucesso: {partition}")

    return partition
