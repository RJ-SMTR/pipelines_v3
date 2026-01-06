# -*- coding: utf-8 -*-
"""Module to deal with the filesystem"""

import json
import os
from importlib.resources import files
from pathlib import Path
from typing import Optional, Union

import pandas as pd

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
        get_project_root_path()
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
    data: Union[str, dict, list[dict], pd.DataFrame],
    csv_mode="w",
):
    """
    Salva um arquivo localmente

    Args:
        filepath (str): Caminho para salvar o arquivo
        filetype (str): Extensão do arquivo
        data Union[str, dict, list[dict], pd.DataFrame]: Dados que serão salvos no arquivo
        csv_mode (str): Modo de escrita no arquivo CSV
    """
    print(f"Saving data on local file: {filepath}")

    print("Creating parent folder...")
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    print("Parent folder created!")

    if isinstance(data, pd.DataFrame):
        print("Received a DataFrame, saving file as CSV")
        if csv_mode == "a":
            data.to_csv(filepath, index=False, header=False, mode="a")
        else:
            data.to_csv(filepath, index=False)
        print("File saved!")
        return

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


def read_raw_data(filepath: str, reader_args: Optional[dict] = None) -> pd.DataFrame:
    """
    Lê os dados de um arquivo Raw

    Args:
        filepath (str): Caminho do arquivo
        reader_args (dict, optional): Argumentos para passar na função
            de leitura (pd.read_csv ou pd.read_json)

    Returns:
        pd.DataFrame: DataFrame com os dados lidos
    """

    print(f"Reading raw data in {filepath}")
    if reader_args is None:
        reader_args = {}

    filetype = get_filetype(filepath=filepath)

    print(f"Reading {filetype.upper()}")
    if filetype == "json":
        data = pd.read_json(filepath, **reader_args)

    elif filetype in ("txt", "csv"):
        data = pd.read_csv(filepath, **reader_args)
    else:
        raise NotImplementedError(
            "Unsupported raw file extension. Supported only: json, csv and txt"
        )

    return data
