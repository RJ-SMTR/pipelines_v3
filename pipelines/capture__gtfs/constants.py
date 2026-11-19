# -*- coding: utf-8 -*-
"""Valores constantes para captura e tratamento de dados do GTFS"""

DATA_GTFS_V2_INICIO = "2025-04-30"
DATA_GTFS_V3_INICIO = "2024-11-06"
DATA_GTFS_V4_INICIO = "2025-07-16"
DATA_GTFS_V5_INICIO = "2025-12-21"

GTFS_CONTROLE_OS_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Jn7fmaDOhuHMdMqHo5SGWHCRuerXNWJRmhRjnHxJ9O4"
    "/pub?gid=0&single=true&output=csv"
)

GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"
GTFS_DISCORD_WEBHOOK = "gtfs"
GTFS_MATERIALIZACAO_DATASET_ID = "gtfs"
PLANEJAMENTO_MATERIALIZACAO_DATASET_ID = "planejamento"

GTFS_TABLE_CAPTURE_PARAMS = {
    "ordem_servico": ["servico", "tipo_os"],
    "ordem_servico_trajeto_alternativo": ["servico", "tipo_os", "evento"],
    "ordem_servico_trajeto_alternativo_sentido": ["servico", "sentido", "tipo_os", "evento"],
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

GTFS_DBT_EXCLUDE = (
    "calendario aux_calendario_manual viagem_planejada_planejamento_dia "
    "matriz_integracao tecnologia_servico "
    "servico_planejado_faixa_horaria matriz_reparticao_tarifaria"
)

GTFS_DBT_TEST_EXCLUDE = (
    "tecnologia_servico sumario_faixa_servico_dia sumario_faixa_servico_dia_pagamento "
    "viagem_planejada viagens_remuneradas sumario_servico_dia_historico "
    "viagem_planejada_planejamento_dia "
    "planejamento_gtfs_freshness__viagem_planejada_planejamento"
)

GTFS_DBT_TEST_SELECT = (
    f"{GTFS_MATERIALIZACAO_DATASET_ID} "
    f"{PLANEJAMENTO_MATERIALIZACAO_DATASET_ID} "
    "test_consistencia_servicos_ordem_servico_gtfs"
)
