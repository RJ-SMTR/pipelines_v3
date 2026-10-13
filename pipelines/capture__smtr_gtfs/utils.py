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
    df = df.dropna(how="all")
    df = df[df["Fim da Vigência da OS"] != "Sem Vigência"]
    df = df[df["Submeter mudanças para Dados"] == True]  # noqa
    df = df[~df[constants.OS_INICIO_VIGENCIA_COLUMN].isnull()]
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


def get_os_sheets(excel_file: pd.ExcelFile, anexo: str) -> list[str]:
    """Retorna as abas da OS de um anexo, como 'ANEXO I '."""
    sheets = [name for name in excel_file.sheet_names if anexo in name]
    if not sheets:
        raise ValueError(f"Nenhuma aba '{anexo.strip()}' encontrada no arquivo.")
    return sheets


def get_tipo_os(sheet_name: str) -> str:
    """Extrai o tipo da OS do nome da aba, como 'Regular' em 'ANEXO I (Regular)'."""
    match = re.search(r"\((.*?)\)", sheet_name)
    if not match:
        raise ValueError(f"Não foi possível extrair tipo_os do nome da aba: {sheet_name}")
    return match.group(1)


def normalize_os_column(column: str) -> str:
    """Normaliza espaços e quebras de linha do nome de uma coluna da OS."""
    column = re.sub(r"\s+", " ", column.replace("\n", " ").strip())
    return column.replace("Dia Útil", "Dias Úteis")


def processa_ordem_servico(excel_file: pd.ExcelFile) -> pd.DataFrame:
    """Processa as abas de Ordem de Serviço de um arquivo Excel."""
    sheets = get_os_sheets(excel_file, "ANEXO I ")
    columns = constants.ORDEM_SERVICO_COLUMNS
    columns_in_values = set(columns.values())
    sheets_data = []

    for sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        tipo_os = get_tipo_os(sheet_name)

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
    sheets = get_os_sheets(excel_file, "ANEXO II ")
    if data_versao_gtfs < constants.DATA_GTFS_V5_INICIO:
        columns = constants.TRAJETO_ALTERNATIVO_COLUMNS
    else:
        columns = constants.TRAJETO_ALTERNATIVO_SENTIDO_COLUMNS
    sheets_data = []

    for sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        tipo_os = get_tipo_os(sheet_name)

        df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=object)
        df = df.rename(columns=columns)
        df["tipo_os"] = tipo_os
        sheets_data.append(df)

    ordem_servico_trajeto_alternativo = pd.concat(sheets_data, ignore_index=True)
    check_os_columns(ordem_servico_trajeto_alternativo, set(columns.values()), table_id)
    return ordem_servico_trajeto_alternativo


def get_faixa_horaria_metrica(metrica: str, data_versao_gtfs: str) -> str:
    """Retorna o prefixo da coluna de uma métrica de faixa horária conforme a versão."""
    if metrica in ["Quilometragem", "KM"]:
        return "quilometragem"
    if (
        metrica == "Partidas" and data_versao_gtfs < constants.DATA_GTFS_V2_INICIO
    ) or data_versao_gtfs >= constants.DATA_GTFS_V4_INICIO:
        return "partidas"
    return "partidas_ida" if metrica == "Partidas Ida" else "partidas_volta"


def get_faixa_horaria_intervalos(data_versao_gtfs: str) -> list[str]:
    """Retorna os intervalos de faixa horária vigentes na versão."""
    if data_versao_gtfs < constants.DATA_GTFS_V3_INICIO:
        return constants.FAIXA_HORARIA_INTERVALOS_V1
    if data_versao_gtfs < constants.DATA_GTFS_V4_INICIO:
        return constants.FAIXA_HORARIA_INTERVALOS_V3
    return constants.FAIXA_HORARIA_INTERVALOS_V4


def get_faixa_horaria_columns(data_versao_gtfs: str) -> dict[str, str]:
    """Monta o mapeamento de colunas da faixa horária conforme a versão do GTFS."""
    intervalo_columns = {
        formato.format(metrica=metrica, intervalo=intervalo, dia=dia): unidecode(
            get_faixa_horaria_metrica(metrica, data_versao_gtfs)
            + "_entre_"
            + intervalo.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
            + "_"
            + dia.lower().replace(" ", "_")
        )
        for metrica in constants.FAIXA_HORARIA_METRICAS
        for intervalo in get_faixa_horaria_intervalos(data_versao_gtfs)
        for dia in constants.FAIXA_HORARIA_DIAS
        for formato in constants.FAIXA_HORARIA_FORMATOS
    }

    if data_versao_gtfs >= constants.DATA_GTFS_V4_INICIO:
        return {
            **intervalo_columns,
            **constants.OS_SERVICO_COLUMNS,
            **constants.OS_SENTIDO_EXTENSAO_COLUMNS,
            **constants.OS_VISTA_COLUMNS,
        }
    if data_versao_gtfs >= constants.DATA_GTFS_V2_INICIO:
        return {**constants.FAIXA_HORARIA_V2_COLUMNS, **intervalo_columns}
    return {**intervalo_columns, **constants.OS_SERVICO_COLUMNS}


def processa_aba_faixa_horaria(
    df: pd.DataFrame, columns: dict[str, str], tipo_os: str
) -> pd.DataFrame:
    """Renomeia e converte as colunas de uma aba de faixa horária."""
    df = df.rename(columns=normalize_os_column).rename(columns=columns)

    for col in df.columns:
        if "quilometragem" in col or "viagens" in col or "partidas" in col:
            df[col] = df[col].astype(str).replace(["—", "-"], 0)
        if "quilometragem" in col or "viagens" in col:
            df[col] = df[col].astype(str).apply(convert_to_float).astype(float)
        if "extensao" in col:
            df[col] = df[col].apply(pd.to_numeric) / 1000
        if "horario" in col:
            df[col] = df[col].astype(str).apply(normalizar_horario)

    df["tipo_os"] = tipo_os

    missing_columns = set(columns.values()) - set(df.columns)
    for coluna in missing_columns:
        if "ponto_facultativo" in coluna:
            df[coluna] = 0

    return df


def processa_ordem_servico_faixa_horaria(
    excel_file: pd.ExcelFile, data_versao_gtfs: str, table_id: str
) -> pd.DataFrame:
    """Processa as abas de Faixa Horária de um arquivo Excel."""
    anexo = "ANEXO I " if data_versao_gtfs >= constants.DATA_GTFS_V2_INICIO else "ANEXO III "
    sheets = get_os_sheets(excel_file, anexo)
    columns = get_faixa_horaria_columns(data_versao_gtfs)
    sheets_data = []

    for sheet_name in sheets:
        print(f"########## {sheet_name} ##########")
        tipo_os = get_tipo_os(sheet_name)
        df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=object)
        sheets_data.append(processa_aba_faixa_horaria(df, columns, tipo_os))

    ordem_servico_faixa_horaria = pd.concat(sheets_data, ignore_index=True)
    check_os_columns(ordem_servico_faixa_horaria, set(columns.values()), table_id)
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
    df[constants.OS_INICIO_VIGENCIA_COLUMN] = pd.to_datetime(
        df[constants.OS_INICIO_VIGENCIA_COLUMN], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%d")
    df["data_index"] = (
        df[constants.OS_INICIO_VIGENCIA_COLUMN].astype(str) + "_" + df["index"].astype(str)
    )
    return df.sort_values(by=[constants.OS_INICIO_VIGENCIA_COLUMN, "index"], ascending=True)


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
    data = {constants.OS_INICIO_VIGENCIA_COLUMN: None, "data_index": None}
    if rows.empty:
        return False, data, None, None

    if data_versao_gtfs is not None:
        selected = rows.loc[rows[constants.OS_INICIO_VIGENCIA_COLUMN] == data_versao_gtfs].tail(1)
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
    return True, data, data["data_index"], data[constants.OS_INICIO_VIGENCIA_COLUMN]


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
        save_local_file(filepath=raw_filepath, filetype="csv", data=data)
    else:
        with zipfile.ZipFile(extra_parameters["gtfs_filepath"]) as zipped_file:
            data = zipped_file.read(f"{table_id}.txt").decode(encoding="utf-8")
        save_local_file(filepath=raw_filepath, filetype="csv", data=data)

    return [raw_filepath]
