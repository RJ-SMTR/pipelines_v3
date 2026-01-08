# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector cadastro
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__jae_auxiliar import constants as jae_auxiliar_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector

CADASTRO_SELECTOR = DBTSelector(
    name="cadastro",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__cadastro",
    data_sources=[
        s
        for s in jae_auxiliar_constants.JAE_AUXILIAR_SOURCES
        if s.table_id
        in [
            "linha",
            "linha_sem_ressarcimento",
            "linha_consorcio",
            "linha_consorcio_operadora_transporte",
            "cliente",
            "gratuidade",
            "escola",
            "laudo_pcd",
            "estudante",
        ]
    ],
)
