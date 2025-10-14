# -*- coding: utf-8 -*-
"""
Valores constantes para pipelines br_rj_riodejaneiro_brt_gps
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para pipelines br_rj_riodejaneiro_brt_gps
    """

    MAX_TIMEOUT_SECONDS = 60
    TIMEZONE = "America/Sao_Paulo"


    GPS_BRT_RAW_DATASET_ID = "br_rj_riodejaneiro_brt_gps"
    GPS_BRT_RAW_TABLE_ID = "registros"
    GPS_BRT_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_BRT_TREATED_TABLE_ID = "gps_brt"
    GPS_BRT_MATERIALIZE_DELAY_HOURS = 0
    GPS_BRT_API_URL = "https://zn4.m2mcontrol.com.br/api/integracao/veiculos"
    GPS_BRT_API_SECRET_PATH = "brt_api_v2"

    GPS_BRT_MAPPING_KEYS = {
        "codigo": "id_veiculo",
        "linha": "servico",
        "latitude": "latitude",
        "longitude": "longitude",
        "dataHora": "timestamp_gps",
        "velocidade": "velocidade",
        "sentido": "sentido",
        "trajeto": "vista",
        # "inicio_viagem": "timestamp_inicio_viagem",
    }
