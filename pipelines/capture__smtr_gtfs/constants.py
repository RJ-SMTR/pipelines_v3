# -*- coding: utf-8 -*-
"""Constantes usadas pela captura do GTFS."""

from pipelines.capture__smtr_gtfs.source_table import GTFSSourceTable

GTFS_CONTROLE_OS_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Jn7fmaDOhuHMdMqHo5SGWHCRuerXNWJRmhRjnHxJ9O4"
    "/pub?gid=0&single=true&output=csv"
)

GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"

GTFS_TABLE_CAPTURE_PARAMS = {
    "ordem_servico": {"primary_keys": ["servico", "tipo_os"]},
    "ordem_servico_trajeto_alternativo": {"primary_keys": ["servico", "tipo_os", "evento"]},
    "ordem_servico_trajeto_alternativo_sentido": {
        "primary_keys": ["servico", "sentido", "tipo_os", "evento"],
    },
    "ordem_servico_faixa_horaria": {"primary_keys": ["servico", "tipo_os"]},
    "ordem_servico_faixa_horaria_sentido": {"primary_keys": ["servico", "sentido", "tipo_os"]},
    "shapes": {"primary_keys": ["shape_id", "shape_pt_sequence"]},
    "agency": {"primary_keys": ["agency_id"]},
    "calendar_dates": {"primary_keys": ["service_id", "date"]},
    "calendar": {"primary_keys": ["service_id"]},
    "feed_info": {"primary_keys": ["feed_publisher_name"], "validate_data_contract": True},
    "frequencies": {"primary_keys": ["trip_id", "start_time"]},
    "routes": {"primary_keys": ["route_id"]},
    "stops": {"primary_keys": ["stop_id"]},
    "trips": {"primary_keys": ["trip_id"]},
    "fare_attributes": {"primary_keys": ["fare_id"]},
    "fare_rules": {"primary_keys": ["fare_id", "route_id"]},
    "stop_times": {"primary_keys": ["trip_id", "stop_sequence"]},
}

DATA_GTFS_V2_INICIO = "2025-04-30"
DATA_GTFS_V3_INICIO = "2024-11-06"
DATA_GTFS_V4_INICIO = "2025-07-16"
DATA_GTFS_V5_INICIO = "2025-12-21"

GTFS_SOURCES = [
    GTFSSourceTable(
        table_id=k,
        dataset_id=GTFS_DATASET_ID,
        primary_keys=v["primary_keys"],
        validate_data_contract=v.get("validate_data_contract", False),
        data_contract_ignored_columns=["data_versao"],
    )
    for k, v in GTFS_TABLE_CAPTURE_PARAMS.items()
]
