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
    "ordem_servico": ["servico", "tipo_os"],
    "ordem_servico_trajeto_alternativo": ["servico", "tipo_os", "evento"],
    "ordem_servico_trajeto_alternativo_sentido": [
        "servico",
        "sentido",
        "tipo_os",
        "evento",
    ],
    "ordem_servico_faixa_horaria": ["servico", "tipo_os"],
    "ordem_servico_faixa_horaria_sentido": ["servico", "sentido", "tipo_os"],
    "shapes": ["shape_id", "shape_pt_sequence"],
    "agency": ["agency_id"],
    "calendar_dates": ["service_id", "date"],
    "calendar": ["service_id"],
    "feed_info": ["feed_publisher_name"],
    "frequencies": ["trip_id", "start_time"],
    "routes": ["route_id"],
    "stops": ["stop_id"],
    "trips": ["trip_id"],
    "fare_attributes": ["fare_id"],
    "fare_rules": ["fare_id", "route_id"],
    "stop_times": ["trip_id", "stop_sequence"],
}

DATA_GTFS_V2_INICIO = "2025-04-30"
DATA_GTFS_V3_INICIO = "2024-11-06"
DATA_GTFS_V4_INICIO = "2025-07-16"
DATA_GTFS_V5_INICIO = "2025-12-21"

GTFS_SOURCES = [
    GTFSSourceTable(table_id=table_id, primary_keys=primary_keys, dataset_id=GTFS_DATASET_ID)
    for table_id, primary_keys in GTFS_TABLE_CAPTURE_PARAMS.items()
]
