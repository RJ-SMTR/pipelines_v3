# -*- coding: utf-8 -*-
"""Funções utilitárias para captura do GTFS"""

import io
import re
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from unidecode import unidecode

from pipelines.capture__smtr_gtfs import constants
from pipelines.common import constants as smtr_constants
from pipelines.common.utils.extractors.api import get_api_data
from pipelines.common.utils.extractors.gdrive import download_drive_file, get_google_api_service
from pipelines.common.utils.fs import get_data_folder_path, save_local_file
from pipelines.common.utils.gcp.storage import Storage


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


def check_os_columns(data: pd.DataFrame, expected_columns: set[str], table_id: str) -> None:
    """Valida se as colunas lidas da OS são conhecidas e não estão duplicadas."""
    columns = set(data.columns)
    print(f"Colunas ausentes em {table_id}: {expected_columns - columns}")
    if not columns.issubset(expected_columns) or len(columns) != len(data.columns):
        print(f"Colunas inesperadas em {table_id}: {columns - expected_columns}")
        raise ValueError(f"Colunas faltantes ou duplicadas em {table_id}")


def processa_ordem_servico(excel_file: pd.ExcelFile) -> pd.DataFrame:
    """Processa as abas de Ordem de Serviço de um arquivo Excel."""
    sheets = [name for name in excel_file.sheet_names if "ANEXO I " in name]
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

    for sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        match = re.search(r"\((.*?)\)", sheet_name)
        if not match:
            raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
        tipo_os = match.group(1)

        quadro = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=object)
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
    check_os_columns(quadro_geral, columns_in_values, "ordem_servico")
    return quadro_geral


def processa_ordem_servico_trajeto_alternativo(
    excel_file: pd.ExcelFile, data_versao_gtfs: str, table_id: str
) -> pd.DataFrame:
    """Processa as abas de Trajetos Alternativos de um arquivo Excel."""
    sheets = [name for name in excel_file.sheet_names if "ANEXO II " in name]
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

    for sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        match = re.search(r"\((.*?)\)", sheet_name)
        if not match:
            raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
        tipo_os = match.group(1)

        df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=object)
        df = df.rename(columns=alt_columns)
        df["tipo_os"] = tipo_os
        sheets_data.append(df)

    ordem_servico_trajeto_alternativo = pd.concat(sheets_data, ignore_index=True)
    check_os_columns(ordem_servico_trajeto_alternativo, set(alt_columns.values()), table_id)
    return ordem_servico_trajeto_alternativo


def processa_ordem_servico_faixa_horaria(  # noqa: PLR0912, PLR0915
    excel_file: pd.ExcelFile, data_versao_gtfs: str, table_id: str
) -> pd.DataFrame:
    """Processa as abas de Faixa Horária de um arquivo Excel."""
    if data_versao_gtfs >= constants.DATA_GTFS_V2_INICIO:
        sheets = [name for name in excel_file.sheet_names if "ANEXO I " in name]
        if not sheets:
            raise ValueError("Nenhuma aba 'ANEXO I' encontrada no arquivo.")
    else:
        sheets = [name for name in excel_file.sheet_names if "ANEXO III " in name]
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

    for sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        match = re.search(r"\((.*?)\)", sheet_name)
        if not match:
            raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
        tipo_os = match.group(1)

        df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=object)
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
    check_os_columns(ordem_servico_faixa_horaria, columns_in_values, table_id)
    return ordem_servico_faixa_horaria


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


def get_os_rows() -> pd.DataFrame:
    """Baixa e ordena as linhas válidas da planilha de controle de OS."""
    df = pd.read_csv(
        io.StringIO(get_api_data(url=constants.GTFS_CONTROLE_OS_URL, raw_filetype="csv"))
    )
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
) -> tuple[bool, dict, str | None, str | None]:
    """Seleciona a próxima OS válida ou uma versão solicitada explicitamente."""
    rows = get_os_rows()
    data = {"Início da Vigência da OS": None, "data_index": None}
    if rows.empty:
        return False, data, None, None

    if data_versao_gtfs is not None:
        selected = rows.loc[rows["Início da Vigência da OS"] == data_versao_gtfs].tail(1)
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

    data = selected.iloc[0].to_dict()
    print(f"OS selecionada: {data}")
    return True, data, data["data_index"], data["Início da Vigência da OS"]


def filter_gtfs_table_ids(
    data_versao_gtfs: str, gtfs_table_capture_params: dict[str, dict]
) -> dict[str, dict]:
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


def download_gtfs_files(
    os_control: dict, data_versao_gtfs: str, upload_from_gcs: bool, env: str
) -> tuple[str, str]:
    """Baixa uma única vez a OS e o ZIP do GTFS para uso de todas as tabelas."""
    if upload_from_gcs:
        print("Baixando arquivos através do GCS")
        storage = Storage(env=env, dataset_id=constants.GTFS_DATASET_ID)
        os_bytes = storage.get_blob_bytes(mode="upload", filename="os", filetype="xlsx")
        gtfs_bytes = storage.get_blob_bytes(mode="upload", filename="gtfs", filetype="zip")
    else:
        print("Baixando arquivos através do Google Drive")
        drive_service = get_google_api_service(service_name="drive", version="v3")
        os_bytes = download_drive_file(os_control["Link da OS"], drive_service)
        gtfs_bytes = download_drive_file(os_control["Link do GTFS"], drive_service)

    folder = Path(get_data_folder_path()) / "upload" / constants.GTFS_DATASET_ID / data_versao_gtfs
    folder.mkdir(parents=True, exist_ok=True)
    os_filepath = folder / "os.xlsx"
    gtfs_filepath = folder / "gtfs.zip"
    os_filepath.write_bytes(os_bytes)
    gtfs_filepath.write_bytes(gtfs_bytes)
    print(f"Arquivos salvos em {folder}")
    return str(os_filepath), str(gtfs_filepath)


def extract_gtfs_table(context) -> list[str]:
    """Extrai a tabela do contexto da OS ou do ZIP do GTFS e salva o arquivo bruto."""
    table_id = context.source.table_id
    extra_parameters = context.extra_parameters
    data_versao_gtfs = extra_parameters["data_versao_gtfs"]
    raw_filepath = context.raw_filepath.format(page=0)

    if table_id.startswith("ordem_servico"):
        excel_file = pd.ExcelFile(extra_parameters["os_filepath"])
        if table_id == "ordem_servico":
            data = processa_ordem_servico(excel_file)
        elif table_id.startswith("ordem_servico_trajeto_alternativo"):
            data = processa_ordem_servico_trajeto_alternativo(
                excel_file, data_versao_gtfs, table_id
            )
        else:
            data = processa_ordem_servico_faixa_horaria(excel_file, data_versao_gtfs, table_id)
        raw_filepath = str(Path(raw_filepath).with_suffix(".csv"))
        save_local_file(filepath=raw_filepath, filetype="csv", data=data)
    else:
        with zipfile.ZipFile(extra_parameters["gtfs_filepath"]) as zipped_file:
            data = zipped_file.read(f"{table_id}.txt").decode(encoding="utf-8")
        save_local_file(filepath=raw_filepath, filetype="txt", data=data)

    return [raw_filepath]
