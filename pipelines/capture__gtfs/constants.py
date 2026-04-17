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
    "calendario aux_calendario_manual viagem_planejada_planejamento "
    "matriz_integracao tecnologia_servico aux_ordem_servico_faixa_horaria "
    "servico_planejado_faixa_horaria matriz_reparticao_tarifaria"
)

GTFS_DBT_TEST_EXCLUDE = (
    "tecnologia_servico sumario_faixa_servico_dia sumario_faixa_servico_dia_pagamento "
    "viagem_planejada viagens_remuneradas sumario_servico_dia_historico"
)

GTFS_DATA_CHECKS_LIST = {
    "calendar_gtfs": {
        "dbt_expectations.expect_column_values_to_match_regex__service_id__calendar_gtfs": {
            "description": "Todos os 'service\\_id' começam com 'U\\_', 'S\\_', 'D\\_' ou 'EXCEP'."
        },
    },
    "ordem_servico_trajeto_alternativo_gtfs": {
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_gtfs": {
            "description": "Todos os dados de 'feed_start_date' e 'tipo_os' correspondem 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_gtfs' e 'ordem_servico_gtfs'."
        },
    },
    "ordem_servico_trajeto_alternativo_sentido": {
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_sentido": {
            "description": "Todos os dados de 'feed_start_date' e 'tipo_os' correspondem 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_sentido' e 'ordem_servico_gtfs'."
        },
    },
    "ordem_servico_trips_shapes_gtfs": {
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trips_shapes_gtfs": {
            "description": "Todos os dados de 'feed_start_date', 'tipo_os', 'tipo_dia', 'servico' e 'faixa_horaria_inicio' correspondem 1:1 entre as tabelas 'ordem_servico_trips_shapes_gtfs' e 'ordem_servico_faixa_horaria'."
        },
        "dbt_utils.unique_combination_of_columns__ordem_servico_trips_shapes_gtfs": {
            "description": "Todos os dados de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'sentido', 'faixa_horaria_inicio' e 'shape_id' são únicos."
        },
        "dbt_expectations.expect_table_row_count_to_be_between__ordem_servico_trips_shapes_gtfs": {
            "description": "A quantidade de registros de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'faixa_horaria_inicio' e 'shape_id' está dentro do intervalo esperado."
        },
    },
    "trips_gtfs": {
        "test_shape_id_gtfs__trips_gtfs": {
            "description": "Todos os `shape_id` de `trips_gtfs` constam na tabela `shapes_gtfs`"
        },
    },
}
