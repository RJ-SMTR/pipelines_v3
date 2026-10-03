# -*- coding: utf-8 -*-
"""Funções utilitárias para captura do GTFS"""

import io
import json
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Union
from zoneinfo import ZoneInfo

import openpyxl as xl
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from googleapiclient.http import MediaIoBaseDownload
from unidecode import unidecode

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.extractors.gdrive import get_google_api_service
from pipelines.common.utils.gcp.bigquery import Dataset, SourceTable
from pipelines.common.utils.gcp.storage import Storage


def get_upload_storage_blob(env: str, dataset_id: str, filename: str):
    """Retorna um blob da zona de upload do GCS."""
    gcs = Storage(env=env, dataset_id=dataset_id)
    blobs = list(gcs.bucket.list_blobs(prefix=f"upload/{dataset_id}/{filename}."))
    if not blobs:
        raise FileNotFoundError(f"Nenhum blob encontrado em upload/{dataset_id}/{filename}")
    return blobs[0]


def xl_load_workbook_sheetnames(file_bytes: io.BytesIO) -> list:
    """Retorna os nomes das abas de um arquivo Excel."""
    file_bytes.seek(0)
    wb = xl.load_workbook(file_bytes)
    names = wb.sheetnames
    file_bytes.seek(0)
    return names


def save_raw_local_func(
    data: Union[dict, str],
    filepath: str,
    mode: str = "raw",
    filetype: str = "json",
) -> str:
    """Salva dados brutos em um arquivo local."""
    _filepath = filepath.format(mode=mode, filetype=filetype)
    Path(_filepath).parent.mkdir(parents=True, exist_ok=True)

    if filetype == "json":
        if isinstance(data, str):
            data = json.loads(data)
        with Path(_filepath).open("w", encoding="utf-8") as fi:
            json.dump(data, fi)

    elif filetype in ("txt", "csv"):
        with Path(_filepath).open("w", encoding="utf-8") as file:
            file.write(data)

    print(f"Raw data saved to: {_filepath}")
    return _filepath


def filter_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra linhas válidas do DataFrame de controle de OS."""
    df["index"] = df.index
    df.dropna(how="all", inplace=True)
    df = df[df["Fim da Vigência da OS"] != "Sem Vigência"]
    df = df[df["Submeter mudanças para Dados"] == True]  # noqa
    df = df[~df["Início da Vigência da OS"].isnull()]
    df = df[~df["Arquivo OS"].isnull()]
    df = df[~df["Arquivo GTFS"].isnull()]
    df = df[~df["Link da OS"].isnull()]
    df = df[~df["Link do GTFS"].isnull()]
    return df


def download_controle_os_csv(url: str) -> pd.DataFrame:
    """Baixa o CSV de controle de OS e retorna como DataFrame."""
    response = requests.get(url=url, timeout=smtr_constants.MAX_TIMEOUT_SECONDS)
    response.raise_for_status()
    response.encoding = "utf-8"
    df = pd.read_csv(io.StringIO(response.text))
    print(f"Download concluído! Dados:\n{df.head()}")
    return df


def convert_to_float(value):
    """Converte um valor string para float."""
    if "," in str(value):
        value = str(value).replace(".", "").replace(",", ".").strip()
    return float(value)


def normalizar_horario(horario: str) -> str:
    """Normaliza uma string de horário."""
    horario = str(horario)
    if "day" in horario:
        days, time = horario.split(", ")
        days = int(days.split(" ")[0])
        hours, minutes, seconds = map(int, time.split(":"))
        total_hours = days * 24 + hours
        return f"{total_hours:02}:{minutes:02}:{seconds:02}"
    else:
        return horario.split(" ")[1] if " " in horario else horario


def download_xlsx(file_link: str, drive_service) -> io.BytesIO:
    """Baixa um arquivo XLSX do Google Drive."""
    file_id = file_link.split("/")[-2]
    file = drive_service.files().get(fileId=file_id, supportsAllDrives=True).execute()
    mime_type = file.get("mimeType")

    if "google-apps" in mime_type:
        request = drive_service.files().export(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = drive_service.files().get_media(fileId=file_id)

    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    file_bytes.seek(0)
    return file_bytes


def download_file(file_link: str, drive_service) -> io.BytesIO:
    """Baixa um arquivo do Google Drive."""
    file_id = file_link.split("/")[-2]
    request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return file_bytes


def processa_ordem_servico(
    sheetnames,
    file_bytes,
    local_filepath,
    raw_filepaths,
    regular_sheet_index=None,  # noqa: ARG001
):
    """Processa as abas de Ordem de Serviço de um arquivo Excel."""
    sheets = [(i, name) for i, name in enumerate(sheetnames) if "ANEXO I " in name]
    if not sheets:
        raise ValueError("Nenhuma aba 'ANEXO I' encontrada no arquivo.")
    sheets_data = []

    columns = {
        "Serviço": "servico",
        "Vista": "vista",
        "Consórcio": "consorcio",
        "Extensão de Ida": "extensao_ida",
        "Extensão de Volta": "extensao_volta",
        "Horário Inicial": "horario_inicio",
        "Horário\nInicial": "horario_inicio",
        "Horário Fim": "horario_fim",
        "Horário\nFim": "horario_fim",
        "Partidas Ida Dia Útil": "partidas_ida_du",
        "Partidas Volta Dia Útil": "partidas_volta_du",
        "Viagens Dia Útil": "viagens_du",
        "Quilometragem Dia Útil": "km_dia_util",
        "KM Dia Útil": "km_dia_util",
        "Partidas Ida Sábado": "partidas_ida_sabado",
        "Partidas Volta Sábado": "partidas_volta_sabado",
        "Viagens Sábado": "viagens_sabado",
        "Quilometragem Sábado": "km_sabado",
        "KM Sábado": "km_sabado",
        "Partidas Ida Domingo": "partidas_ida_domingo",
        "Partidas Volta Domingo": "partidas_volta_domingo",
        "Viagens Domingo": "viagens_domingo",
        "Quilometragem Domingo": "km_domingo",
        "KM Domingo": "km_domingo",
        "Partidas Ida Ponto Facultativo": "partidas_ida_pf",
        "Partidas Volta Ponto Facultativo": "partidas_volta_pf",
        "Viagens Ponto Facultativo": "viagens_pf",
        "Quilometragem Ponto Facultativo": "km_pf",
        "KM Ponto Facultativo": "km_pf",
        "tipo_os": "tipo_os",
    }
    columns_in_values = set(columns.values())

    for _, sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        match = re.search(r"\((.*?)\)", sheet_name)
        if not match:
            raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
        tipo_os = match.group(1)

        quadro = pd.read_excel(file_bytes, sheet_name=sheet_name, dtype=object)
        quadro = quadro.rename(columns=columns)
        quadro["servico"] = quadro["servico"].astype(str)
        quadro["servico"] = quadro["servico"].str.extract(r"([A-Z]+)", expand=False).fillna(
            ""
        ) + quadro["servico"].str.extract(r"([0-9]+)", expand=False).fillna("")
        quadro["tipo_os"] = tipo_os
        quadro = quadro[list(columns_in_values)]
        quadro = quadro.replace("—", 0)
        quadro = quadro.reindex(columns=list(columns_in_values))

        hora_cols = [coluna for coluna in quadro.columns if "horario" in coluna]
        quadro[hora_cols] = quadro[hora_cols].astype(str)
        for hora_col in hora_cols:
            quadro[hora_col] = quadro[hora_col].apply(normalizar_horario)

        cols = [
            coluna
            for coluna in quadro.columns
            if "km" in coluna or "viagens" in coluna or "partida" in coluna
        ]
        for col in cols:
            quadro[col] = quadro[col].astype(str).apply(convert_to_float).astype(float).fillna(0)

        extensao_cols = ["extensao_ida", "extensao_volta"]
        quadro[extensao_cols] = quadro[extensao_cols].astype(str)
        for col in extensao_cols:
            quadro[col] = quadro[col].str.replace(".", "", regex=False)
        quadro[extensao_cols] = quadro[extensao_cols].apply(pd.to_numeric)
        quadro["extensao_ida"] = quadro["extensao_ida"] / 1000
        quadro["extensao_volta"] = quadro["extensao_volta"] / 1000

        sheets_data.append(quadro)

    quadro_geral = pd.concat(sheets_data, ignore_index=True)

    columns_in_dataframe = set(quadro_geral.columns)
    all_columns_present = columns_in_dataframe.issubset(columns_in_values)
    no_duplicate_columns = len(columns_in_dataframe) == len(quadro_geral.columns)
    missing_columns = columns_in_values - columns_in_dataframe

    print(
        f"All columns present: {all_columns_present}/"
        f"No duplicate columns: {no_duplicate_columns}/"
        f"Missing columns: {missing_columns}"
    )

    if not all_columns_present or not no_duplicate_columns:
        raise Exception("Missing or duplicated columns in ordem_servico")

    local_file_path = next(filter(lambda x: "ordem_servico/" in x, local_filepath))
    quadro_geral_csv = quadro_geral.to_csv(index=False)
    raw_file_path = save_raw_local_func(
        data=quadro_geral_csv, filepath=local_file_path, filetype="csv"
    )
    print(f"Saved file: {raw_file_path}")
    raw_filepaths.append(raw_file_path)


def processa_ordem_servico_trajeto_alternativo(  # noqa: PLR0913
    sheetnames,
    file_bytes,
    local_filepath,
    raw_filepaths,
    data_versao_gtfs,
    filename,
):
    """Processa as abas de Trajetos Alternativos de um arquivo Excel."""
    sheets = [(i, name) for i, name in enumerate(sheetnames) if "ANEXO II " in name]
    if not sheets:
        raise ValueError("Nenhuma aba 'ANEXO II' encontrada no arquivo.")
    sheets_data = []

    if data_versao_gtfs < constants.DATA_GTFS_V5_INICIO:
        alt_columns = {
            "Serviço": "servico",
            "Vista": "vista",
            "Consórcio": "consorcio",
            "Extensão de Ida": "extensao_ida",
            "Extensão\nde Ida": "extensao_ida",
            "Extensão de Volta": "extensao_volta",
            "Extensão\nde Volta": "extensao_volta",
            "Evento": "evento",
            "Horário Inicial Interdição": "inicio_periodo",
            "Horário Final Interdição": "fim_periodo",
            "Descrição": "descricao",
            "Ativação": "ativacao",
            "tipo_os": "tipo_os",
        }
    else:
        alt_columns = {
            "Serviço": "servico",
            "Vista": "vista",
            "Sentido": "sentido",
            "Extensão": "extensao",
            "Consórcio": "consorcio",
            "Evento": "evento",
            "Descrição": "descricao",
            "Ativação": "ativacao",
            "tipo_os": "tipo_os",
        }

    for _, sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        match = re.search(r"\((.*?)\)", sheet_name)
        if not match:
            raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
        tipo_os = match.group(1)

        df = pd.read_excel(file_bytes, sheet_name=sheet_name, dtype=object)
        df = df.rename(columns=alt_columns)
        df["tipo_os"] = tipo_os
        sheets_data.append(df)

    ordem_servico_trajeto_alternativo = pd.concat(sheets_data, ignore_index=True)
    columns_in_dataframe = set(ordem_servico_trajeto_alternativo.columns)
    columns_in_values = set(alt_columns.values())
    all_columns_present = columns_in_dataframe.issubset(columns_in_values)
    no_duplicate_columns = len(columns_in_dataframe) == len(
        ordem_servico_trajeto_alternativo.columns
    )
    missing_columns = columns_in_values - columns_in_dataframe

    print(
        f"All columns present: {all_columns_present}/"
        f"No duplicate columns: {no_duplicate_columns}/"
        f"Missing columns: {missing_columns}"
    )

    if not all_columns_present or not no_duplicate_columns:
        raise Exception("Missing or duplicated columns in ordem_servico_trajeto_alternativo")

    local_file_path = next(filter(lambda x: filename + "/" in x, local_filepath))
    csv_data = ordem_servico_trajeto_alternativo.to_csv(index=False)
    raw_file_path = save_raw_local_func(data=csv_data, filepath=local_file_path, filetype="csv")
    print(f"Saved file: {raw_file_path}")
    raw_filepaths.append(raw_file_path)


def processa_ordem_servico_faixa_horaria(  # noqa: PLR0912, PLR0915, PLR0913
    sheetnames,
    file_bytes,
    local_filepath,
    raw_filepaths,
    data_versao_gtfs,
    filename,
):
    """Processa as abas de Faixa Horária de um arquivo Excel."""
    if data_versao_gtfs >= constants.DATA_GTFS_V2_INICIO:
        sheets = [(i, name) for i, name in enumerate(sheetnames) if "ANEXO I " in name]
        if not sheets:
            raise ValueError("Nenhuma aba 'ANEXO I' encontrada no arquivo.")
    else:
        sheets = [(i, name) for i, name in enumerate(sheetnames) if "ANEXO III " in name]
        if not sheets:
            raise ValueError("Nenhuma aba 'ANEXO III' encontrada no arquivo.")
    sheets_data = []

    columns = {
        "Serviço": "servico",
        "Vista": "vista",
        "Consórcio": "consorcio",
        "Extensão de Ida": "extensao_ida",
        "Extensão de Volta": "extensao_volta",
        "Horário Inicial Dias Úteis": "horario_inicio_dias_uteis",
        "Horário Fim Dias Úteis": "horario_fim_dias_uteis",
        "Horário Inicial - Dias Úteis": "horario_inicio_dias_uteis",
        "Horário Fim - Dias Úteis": "horario_fim_dias_uteis",
        "Partidas Ida - Dias Úteis": "partidas_ida_dias_uteis",
        "Partidas Volta - Dias Úteis": "partidas_volta_dias_uteis",
        "Viagens - Dias Úteis": "viagens_dias_uteis",
        "Quilometragem - Dias Úteis": "quilometragem_dias_uteis",
        "KM - Dias Úteis": "quilometragem_dias_uteis",
        "Horário Inicial Sábado": "horario_inicio_sabado",
        "Horário Fim Sábado": "horario_fim_sabado",
        "Horário Inicial - Sábado": "horario_inicio_sabado",
        "Horário Fim - Sábado": "horario_fim_sabado",
        "Partidas Ida - Sábado": "partidas_ida_sabado",
        "Partidas Volta - Sábado": "partidas_volta_sabado",
        "Viagens - Sábado": "viagens_sabado",
        "Quilometragem - Sábado": "quilometragem_sabado",
        "KM - Sábado": "quilometragem_sabado",
        "Horário Inicial Domingo": "horario_inicio_domingo",
        "Horário Fim Domingo": "horario_fim_domingo",
        "Horário Inicial - Domingo": "horario_inicio_domingo",
        "Horário Fim - Domingo": "horario_fim_domingo",
        "Partidas Ida - Domingo": "partidas_ida_domingo",
        "Partidas Volta - Domingo": "partidas_volta_domingo",
        "Viagens - Domingo": "viagens_domingo",
        "Quilometragem - Domingo": "quilometragem_domingo",
        "KM - Domingo": "quilometragem_domingo",
        "Horário Inicial Ponto Facultativo": "horario_inicio_ponto_facultativo",
        "Horário Fim Ponto Facultativo": "horario_fim_ponto_facultativo",
        "Horário Inicial - Ponto Facultativo": "horario_inicio_ponto_facultativo",
        "Horário Fim - Ponto Facultativo": "horario_fim_ponto_facultativo",
        "Partidas Ida - Ponto Facultativo": "partidas_ida_ponto_facultativo",
        "Partidas Volta - Ponto Facultativo": "partidas_volta_ponto_facultativo",
        "Viagens - Ponto Facultativo": "viagens_ponto_facultativo",
        "Quilometragem - Ponto Facultativo": "quilometragem_ponto_facultativo",
        "KM - Ponto Facultativo": "quilometragem_ponto_facultativo",
        "tipo_os": "tipo_os",
    }

    metricas = ["Partidas", "Partidas Ida", "Partidas Volta", "Quilometragem", "KM"]
    dias = ["Dias Úteis", "Sábado", "Domingo", "Ponto Facultativo"]
    formatos = [
        "{metrica} entre {intervalo} — {dia}",
        "{metrica} entre {intervalo} - {dia}",
        "{metrica} entre {intervalo} ({dia})",
        "{metrica} {intervalo} - {dia}",
    ]

    if data_versao_gtfs < constants.DATA_GTFS_V3_INICIO:
        intervalos = [
            "00h e 03h",
            "03h e 12h",
            "12h e 21h",
            "21h e 24h",
            "24h e 03h (dia seguinte)",
        ]
    elif (
        data_versao_gtfs >= constants.DATA_GTFS_V3_INICIO
        and data_versao_gtfs < constants.DATA_GTFS_V4_INICIO
    ):
        intervalos = [
            "00h e 03h",
            "03h e 06h",
            "06h e 09h",
            "09h e 12h",
            "12h e 15h",
            "15h e 18h",
            "18h e 21h",
            "21h e 24h",
            "24h e 03h (dia seguinte)",
        ]
    else:
        intervalos = [
            "00h à 01h",
            "01h à 02h",
            "02h à 03h",
            "03h à 04h",
            "04h à 05h",
            "05h à 06h",
            "06h à 09h",
            "09h à 12h",
            "12h à 15h",
            "15h à 18h",
            "18h à 21h",
            "21h à 22h",
            "22h à 23h",
            "23h à 24h",
        ]

    fh_columns = {
        formato.format(metrica=metrica, intervalo=intervalo, dia=dia): unidecode(
            (
                "quilometragem"
                if metrica in ["Quilometragem", "KM"]
                else (
                    "partidas"
                    if (metrica == "Partidas" and data_versao_gtfs < constants.DATA_GTFS_V2_INICIO)
                    or data_versao_gtfs >= constants.DATA_GTFS_V4_INICIO
                    else ("partidas_ida" if metrica == "Partidas Ida" else "partidas_volta")
                )
            )
            + "_entre_"
            + intervalo.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
            + "_"
            + dia.lower().replace(" ", "_")
        )
        for metrica in metricas
        for intervalo in intervalos
        for dia in dias
        for formato in formatos
    }

    if data_versao_gtfs >= constants.DATA_GTFS_V4_INICIO:
        fh_columns["Serviço"] = "servico"
        fh_columns["Consórcio"] = "consorcio"
        fh_columns["tipo_os"] = "tipo_os"
        fh_columns["Sentido"] = "sentido"
        fh_columns["Extensão"] = "extensao"
        fh_columns["Vista"] = "vista"
        columns = fh_columns.copy()
    elif data_versao_gtfs >= constants.DATA_GTFS_V2_INICIO:
        columns.update(fh_columns)
    else:
        fh_columns["Serviço"] = "servico"
        fh_columns["Consórcio"] = "consorcio"
        fh_columns["tipo_os"] = "tipo_os"
        columns = fh_columns.copy()

    columns_in_values = set(columns.values())

    for _, sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        match = re.search(r"\((.*?)\)", sheet_name)
        if not match:
            raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
        tipo_os = match.group(1)

        df = pd.read_excel(file_bytes, sheet_name=sheet_name, dtype=object)
        df.columns = (
            df.columns.str.replace("\n", " ").str.strip().str.replace(r"\s+", " ", regex=True)
        )
        df = df.rename(columns=lambda x: x.replace("Dia Útil", "Dias Úteis"))
        df = df.rename(columns=columns)

        for col in df.columns:
            if "quilometragem" in col or "viagens" in col or "partidas" in col:
                df[col] = df[col].astype(str).replace(["—", "-"], 0)
            if "quilometragem" in col or "viagens" in col:
                df[col] = df[col].astype(str).apply(convert_to_float).astype(float)
            if "extensao" in col:
                df[col] = df[col].apply(pd.to_numeric)
                df[col] = df[col] / 1000
            if "horario" in col:
                df[col] = df[col].astype(str)
                df[col] = df[col].apply(normalizar_horario)

        df["tipo_os"] = tipo_os

        aux_columns_in_dataframe = set(df.columns)
        aux_missing_columns = columns_in_values - aux_columns_in_dataframe
        for coluna in aux_missing_columns:
            if "ponto_facultativo" in coluna:
                df[coluna] = 0

        sheets_data.append(df)

    ordem_servico_faixa_horaria = pd.concat(sheets_data, ignore_index=True)
    columns_in_dataframe = set(ordem_servico_faixa_horaria.columns)
    missing_columns = columns_in_values - columns_in_dataframe
    all_columns_present = columns_in_dataframe.issubset(columns_in_values)
    no_duplicate_columns = len(columns_in_dataframe) == len(ordem_servico_faixa_horaria.columns)

    print(
        f"All columns present: {all_columns_present}\n"
        f"No duplicate columns: {no_duplicate_columns}\n"
        f"Missing columns: {missing_columns}"
    )

    if not all_columns_present or not no_duplicate_columns:
        print(columns_in_values.difference(columns_in_dataframe))
        print(columns_in_dataframe.difference(columns_in_values))
        raise Exception("Missing or duplicated columns in ordem_servico_faixa_horaria")

    local_file_path = next(filter(lambda x: filename + "/" in x, local_filepath))
    csv_data = ordem_servico_faixa_horaria.to_csv(index=False)
    raw_file_path = save_raw_local_func(data=csv_data, filepath=local_file_path, filetype="csv")
    print(f"Saved file: {raw_file_path}")
    raw_filepaths.append(raw_file_path)


def redis_state_key(dataset_id: str, state_name: str, mode: str) -> str:
    """Monta a chave Redis de estado respeitando o namespace por ambiente."""
    key = f"{dataset_id}.{state_name}"
    return key if mode == "prod" else f"{mode}.{key}"


def normalize_data_index(value, field_name: str) -> str | None:
    """Normaliza marcadores de OS do Redis, inclusive o formato legado de data."""
    if isinstance(value, dict):
        value = value.get(field_name)
    if value is None:
        return None

    value = str(value)
    date, separator, index = value.partition("_")
    if separator and "/" in date:
        value = (
            datetime.strptime(date, "%d/%m/%Y")
            .replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
            .strftime("%Y-%m-%d")
            + f"_{index}"
        )
    return value


def read_os_marker(redis_client, dataset_id: str, state_name: str, mode: str) -> str | None:
    """Lê e normaliza um marcador de OS do Redis."""
    return normalize_data_index(
        redis_client.get(redis_state_key(dataset_id, state_name, mode)), state_name
    )


def write_os_marker(
    redis_client, dataset_id: str, state_name: str, data_index: str, mode: str
) -> None:
    """Persiste um marcador de OS no Redis."""
    redis_client.set(
        redis_state_key(dataset_id, state_name, mode),
        {state_name: data_index},
    )


def get_os_rows() -> pd.DataFrame:
    """Baixa e ordena as linhas válidas da planilha de controle de OS."""
    df = download_controle_os_csv(constants.GTFS_CONTROLE_OS_URL)
    if df.empty:
        return df

    df = filter_valid_rows(df)
    df["Início da Vigência da OS"] = pd.to_datetime(
        df["Início da Vigência da OS"], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%d")
    df["data_index"] = df["Início da Vigência da OS"].astype(str) + "_" + df["index"].astype(str)
    return df.sort_values(by=["Início da Vigência da OS", "index"], ascending=True)


def data_index_sort_key(data_index: str) -> tuple[str, int]:
    """Produz uma chave ordenável para data e índice da OS."""
    date, separator, index = str(data_index).partition("_")
    if not separator:
        raise ValueError(f"Marcador de OS inválido: {data_index}")
    return date, int(index)


def data_index_is_after(data_index: str, cursor: str) -> bool:
    """Indica se uma OS está depois do cursor informado."""
    return data_index_sort_key(data_index) > data_index_sort_key(cursor)


def next_data_index(rows: pd.DataFrame, last_data_index: str | None) -> str | None:
    """Retorna a primeira OS posterior ao cursor."""
    if rows.empty:
        return None
    if last_data_index is None:
        return rows.iloc[-1]["data_index"]

    cursor = data_index_sort_key(last_data_index)
    pending = rows.loc[rows["data_index"].map(lambda value: data_index_sort_key(value) > cursor)]
    return None if pending.empty else pending.iloc[0]["data_index"]


def get_os_info(
    last_captured_os: str | None = None,
    data_versao_gtfs: str | None = None,
    rows: pd.DataFrame | None = None,
) -> tuple[bool, dict, str | None, str | None]:
    """Seleciona a próxima OS válida ou uma versão solicitada explicitamente."""
    rows = get_os_rows() if rows is None else rows
    data = {"Início da Vigência da OS": None, "data_index": None}
    if rows.empty:
        return False, data, None, None

    if data_versao_gtfs is not None:
        selected = rows.loc[rows["Início da Vigência da OS"] == data_versao_gtfs]
    elif last_captured_os is None:
        selected = rows.tail(1)
    else:
        selected_index = next_data_index(rows, last_captured_os)
        selected = (
            rows.loc[rows["data_index"] == selected_index] if selected_index else rows.head(0)
        )

    if selected.empty:
        print("Nenhuma nova OS encontrada.")
        return False, data, None, None

    row = selected.iloc[0]
    position = rows.index.get_loc(row.name)
    data = row.to_dict()
    data["previous_data_index"] = rows.iloc[position - 1]["data_index"] if position else None
    print(f"OS selecionada: {data}")
    return True, data, data["data_index"], data["Início da Vigência da OS"]


def filter_gtfs_table_ids(
    data_versao_gtfs: str, gtfs_table_capture_params: dict[str, list[str]]
) -> dict[str, list[str]]:
    """Filtra as tabelas disponíveis conforme a versão do formato GTFS."""
    if data_versao_gtfs >= constants.DATA_GTFS_V2_INICIO:
        gtfs_table_capture_params.pop("ordem_servico", None)
    if data_versao_gtfs < constants.DATA_GTFS_V4_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_faixa_horaria_sentido", None)
    if data_versao_gtfs >= constants.DATA_GTFS_V4_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_faixa_horaria", None)
    if data_versao_gtfs < constants.DATA_GTFS_V5_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_trajeto_alternativo_sentido", None)
    if data_versao_gtfs >= constants.DATA_GTFS_V5_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_trajeto_alternativo", None)
    return gtfs_table_capture_params


def get_raw_gtfs_files(  # noqa: PLR0913
    os_control: dict,
    local_filepath: list[str],
    regular_sheet_index: int | None,
    upload_from_gcs: bool,
    data_versao_gtfs: str,
    dict_gtfs: dict[str, list[str]],
    env: str,
) -> list[str]:
    """Baixa o ZIP e a OS, processando um arquivo bruto para cada tabela selecionada."""
    raw_filepaths = []
    print(f"Baixando arquivos: {os_control}")

    if upload_from_gcs:
        print("Baixando arquivos através do GCS")
        file_bytes_os = io.BytesIO(
            get_upload_storage_blob(
                env=env, dataset_id=constants.GTFS_DATASET_ID, filename="os"
            ).download_as_bytes()
        )
        file_bytes_gtfs = io.BytesIO(
            get_upload_storage_blob(
                env=env, dataset_id=constants.GTFS_DATASET_ID, filename="gtfs"
            ).download_as_bytes()
        )
    else:
        print("Baixando arquivos através do Google Drive")
        drive_service = get_google_api_service(service_name="drive", version="v3")
        file_bytes_os = download_xlsx(
            file_link=os_control["Link da OS"], drive_service=drive_service
        )
        file_bytes_gtfs = download_file(
            file_link=os_control["Link do GTFS"], drive_service=drive_service
        )

    sheetnames = [name for name in xl_load_workbook_sheetnames(file_bytes_os) if "ANEXO" in name]
    print(f"tabs encontradas na planilha Controle OS: {sheetnames}")

    with zipfile.ZipFile(file_bytes_gtfs, "r") as zipped_file:
        for filename in dict_gtfs:
            if filename == "ordem_servico":
                processa_ordem_servico(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    regular_sheet_index=regular_sheet_index,
                )
            elif "ordem_servico_trajeto_alternativo" in filename:
                processa_ordem_servico_trajeto_alternativo(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    data_versao_gtfs=data_versao_gtfs,
                    filename=filename,
                )
            elif "ordem_servico_faixa_horaria" in filename:
                processa_ordem_servico_faixa_horaria(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    data_versao_gtfs=data_versao_gtfs,
                    filename=filename,
                )
            else:
                data = zipped_file.read(filename + ".txt").decode(encoding="utf-8")
                local_file_path = next(filter(lambda path: filename + "/" in path, local_filepath))
                raw_file_path = save_raw_local_func(
                    data=data, filepath=local_file_path, filetype="txt"
                )
                print(f"Saved file: {raw_file_path}")
                raw_filepaths.append(raw_file_path)

    return raw_filepaths


def prepare_gtfs_raw_files(context) -> str:
    """Prepara o arquivo bruto da fonte dentro do extractor genérico."""
    extra_parameters = context.extra_parameters
    local_filepath = str(Path(context.raw_filepath.format(page=0)).with_suffix(".{filetype}"))
    raw_filepaths = get_raw_gtfs_files(
        os_control=extra_parameters["os_control"],
        local_filepath=[local_filepath],
        regular_sheet_index=extra_parameters["regular_sheet_index"],
        upload_from_gcs=extra_parameters["upload_from_gcs"],
        data_versao_gtfs=extra_parameters["data_versao_gtfs"],
        dict_gtfs={context.source.table_id: context.source.primary_keys},
        env=context.source.env,
    )
    if len(raw_filepaths) != 1:
        raise ValueError(
            f"Esperado um arquivo bruto para {context.source.table_id}, "
            f"recebidos: {len(raw_filepaths)}"
        )
    return raw_filepaths[0]


def get_prepared_gtfs_raw_file(raw_filepath: str) -> list[str]:
    """Adapta um caminho GTFS preparado ao contrato do extractor genérico."""
    return [raw_filepath]


def normalize_gtfs_data(data: pd.DataFrame, context) -> pd.DataFrame:
    """Normaliza texto e preenche o tipo de OS antes da estrutura aninhada."""
    object_columns = data.select_dtypes(include=["object"]).columns
    data[object_columns] = data[object_columns].apply(lambda column: column.str.strip())

    if "ordem_servico" in context.source.table_id and "tipo_os" not in data.columns:
        data["tipo_os"] = "Regular"
    return data


class GtfsSourceTable(SourceTable):
    """SourceTable que mantém o layout de staging e a partição Hive do GTFS."""

    def __init__(self, table_id: str, primary_keys: list[str], dataset_id: str) -> None:
        self.staging_dataset_id = f"{dataset_id}_staging"
        super().__init__(
            source_name="gtfs",
            table_id=table_id,
            first_timestamp=datetime(2000, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
            flow_folder_name="capture__smtr_gtfs",
            primary_keys=primary_keys,
            pretreatment_reader_args={"dtype": str, "on_bad_lines": "warn"},
            pretreat_funcs=[normalize_gtfs_data],
            partition_date_only=True,
            raw_filetype="txt",
            file_chunk_size=50_000,
            transform_in_chunks=True,
            partition_key="data_versao",
        )
        self.dataset_id = dataset_id
        self.set_env(self.env)

    def set_env(self, env: str):
        super().set_env(env=env)
        self.table_full_name = (
            f"{smtr_constants.PROJECT_NAME[env]}.{self.staging_dataset_id}.{self.table_id}"
        )
        return self

    def _create_table_config(self, sample_filepath: str) -> bigquery.ExternalConfig:
        external_config = bigquery.ExternalConfig("CSV")
        external_config.options.skip_leading_rows = 1
        external_config.options.allow_quoted_newlines = True
        external_config.autodetect = False
        external_config.schema = self._create_table_schema(sample_filepath=sample_filepath)
        external_config.options.field_delimiter = ","
        external_config.options.allow_jagged_rows = False

        source_uri_prefix = f"gs://{self.bucket_name}/staging/{self.dataset_id}/{self.table_id}/"
        external_config.source_uris = [f"{source_uri_prefix}*/*"]
        hive_partitioning = HivePartitioningOptions()
        hive_partitioning.mode = "AUTO"
        hive_partitioning.source_uri_prefix = source_uri_prefix
        external_config.hive_partitioning = hive_partitioning
        return external_config

    def create(self, sample_filepath: str, location: str = "US") -> None:
        Dataset(dataset_id=self.staging_dataset_id, env=self.env, location=location).create()
        table = bigquery.Table(self.table_full_name)
        table.description = f"staging table for `{self.table_full_name}`"
        table.external_data_configuration = self._create_table_config(
            sample_filepath=sample_filepath
        )
        self.client("bigquery").create_table(table)

    def append(self, source_filepath: str, partition: str, if_exists: str = "replace") -> None:
        Storage(
            env=self.env,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_names=self.bucket_names,
        ).upload_file(
            mode="staging",
            filepath=source_filepath,
            partition=partition,
            if_exists=if_exists,
        )


from pipelines.capture__smtr_gtfs import constants  # noqa: E402
