# -*- coding: utf-8 -*-
"""Constantes usadas pela captura do GTFS."""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.pretreatment import strip_string_columns

GTFS_CONTROLE_OS_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Jn7fmaDOhuHMdMqHo5SGWHCRuerXNWJRmhRjnHxJ9O4"
    "/pub?gid=0&single=true&output=csv"
)

GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"
GTFS_GOOGLE_CHAT_WEBHOOK_KEY = "google_chat_planejamento"

GTFS_TABLE_CAPTURE_PARAMS = {
    "ordem_servico": {"primary_keys": ["servico", "tipo_os"]},
    "ordem_servico_trajeto_alternativo": {"primary_keys": ["servico", "tipo_os", "evento"]},
    "ordem_servico_trajeto_alternativo_sentido": {
        "primary_keys": ["servico", "sentido", "tipo_os", "evento"],
        "validate_data_contract": True,
    },
    "ordem_servico_faixa_horaria": {"primary_keys": ["servico", "tipo_os"]},
    "ordem_servico_faixa_horaria_sentido": {
        "primary_keys": ["servico", "sentido", "tipo_os"],
        "validate_data_contract": True,
    },
    "shapes": {"primary_keys": ["shape_id", "shape_pt_sequence"], "validate_data_contract": True},
    "agency": {"primary_keys": ["agency_id"]},
    "calendar_dates": {"primary_keys": ["service_id", "date"]},
    "calendar": {"primary_keys": ["service_id"], "validate_data_contract": True},
    "feed_info": {"primary_keys": ["feed_publisher_name"], "validate_data_contract": True},
    "frequencies": {"primary_keys": ["trip_id", "start_time"]},
    "routes": {"primary_keys": ["route_id"]},
    "stops": {"primary_keys": ["stop_id"]},
    "trips": {
        "primary_keys": ["trip_id"],
        "validate_data_contract": True,
        "data_contract_ignored_columns": ["block_id", "wheelchair_accessible", "bikes_allowed"],
    },
    "fare_attributes": {"primary_keys": ["fare_id"]},
    "fare_rules": {"primary_keys": ["fare_id", "route_id"]},
    "stop_times": {"primary_keys": ["trip_id", "stop_sequence"]},
}

DATA_GTFS_V2_INICIO = "2025-04-30"
DATA_GTFS_V3_INICIO = "2024-11-06"
DATA_GTFS_V4_INICIO = "2025-07-16"
DATA_GTFS_V5_INICIO = "2025-12-21"

OS_INICIO_VIGENCIA_COLUMN = "Início da Vigência da OS"

OS_SERVICO_COLUMNS = {"Serviço": "servico", "Consórcio": "consorcio", "tipo_os": "tipo_os"}
OS_VISTA_COLUMNS = {"Vista": "vista"}
OS_EXTENSAO_IDA_VOLTA_COLUMNS = {
    "Extensão de Ida": "extensao_ida",
    "Extensão de Volta": "extensao_volta",
}
OS_SENTIDO_EXTENSAO_COLUMNS = {"Sentido": "sentido", "Extensão": "extensao"}
OS_EVENTO_COLUMNS = {"Evento": "evento", "Descrição": "descricao", "Ativação": "ativacao"}

ORDEM_SERVICO_COLUMNS = {
    **OS_SERVICO_COLUMNS,
    **OS_VISTA_COLUMNS,
    **OS_EXTENSAO_IDA_VOLTA_COLUMNS,
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
}

TRAJETO_ALTERNATIVO_COLUMNS = {
    **OS_SERVICO_COLUMNS,
    **OS_VISTA_COLUMNS,
    **OS_EXTENSAO_IDA_VOLTA_COLUMNS,
    **OS_EVENTO_COLUMNS,
    "Extensão\nde Ida": "extensao_ida",
    "Extensão\nde Volta": "extensao_volta",
    "Horário Inicial Interdição": "inicio_periodo",
    "Horário Final Interdição": "fim_periodo",
}

TRAJETO_ALTERNATIVO_SENTIDO_COLUMNS = {
    **OS_SERVICO_COLUMNS,
    **OS_VISTA_COLUMNS,
    **OS_SENTIDO_EXTENSAO_COLUMNS,
    **OS_EVENTO_COLUMNS,
}

FAIXA_HORARIA_V2_COLUMNS = {
    **OS_SERVICO_COLUMNS,
    **OS_VISTA_COLUMNS,
    **OS_EXTENSAO_IDA_VOLTA_COLUMNS,
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
}

FAIXA_HORARIA_METRICAS = ["Partidas", "Partidas Ida", "Partidas Volta", "Quilometragem", "KM"]
FAIXA_HORARIA_DIAS = ["Dias Úteis", "Sábado", "Domingo", "Ponto Facultativo"]
FAIXA_HORARIA_FORMATOS = [
    "{metrica} entre {intervalo} — {dia}",
    "{metrica} entre {intervalo} - {dia}",
    "{metrica} entre {intervalo} ({dia})",
    "{metrica} {intervalo} - {dia}",
]
FAIXA_HORARIA_INTERVALOS_V1 = [
    "00h e 03h",
    "03h e 12h",
    "12h e 21h",
    "21h e 24h",
    "24h e 03h (dia seguinte)",
]
FAIXA_HORARIA_INTERVALOS_V3 = [
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
FAIXA_HORARIA_INTERVALOS_V4 = [
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

GTFS_SOURCES = [
    SourceTable(
        source_name="gtfs",
        table_id=k,
        first_timestamp=datetime(2000, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
        flow_folder_name="capture__smtr_gtfs",
        primary_keys=v["primary_keys"],
        pretreatment_reader_args={"dtype": str, "chunksize": 50_000},
        pretreat_funcs=[strip_string_columns],
        partition_date_only=True,
        raw_filetype="csv",
        partition_key="data_versao",
        validate_data_contract=v.get("validate_data_contract", False),
        data_contract_ignored_columns=["data_versao", *v.get("data_contract_ignored_columns", [])],
    )
    for k, v in GTFS_TABLE_CAPTURE_PARAMS.items()
]
