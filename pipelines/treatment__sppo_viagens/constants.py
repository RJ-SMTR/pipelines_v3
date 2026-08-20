# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do seletor viagens_sppo
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

VIAGENS_SPPO_CHECKS_LIST = {
    "viagem_planejada": {
        "unique__snapshot_key__viagem_planejada": {
            "description": (
                "Todos os registros de viagem_planejada são únicos na chave do "
                "snapshot_viagem_planejada (concat de data, servico, sentido, "
                "faixa_horaria_inicio, trip_id, trip_id_planejado, shape_id e "
                "shape_id_planejado)."
            )
        },
    },
}

VIAGENS_SPPO_POST_TEST = DBTTest(
    test_select="unique__snapshot_key__viagem_planejada",
    test_descriptions=VIAGENS_SPPO_CHECKS_LIST,
    truncate_date=True,
)

TZ = ZoneInfo(smtr_constants.TIMEZONE)
VIAGENS_SPPO_INITIAL_DATETIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=TZ)
# O selector usa run_date para materializar D-1; 01/08 é a última run necessária
# para produzir as viagens iniciadas em 31/07.
VIAGENS_SPPO_FINAL_DATETIME = datetime(2026, 8, 2, 0, 0, 0, tzinfo=TZ)

VIAGENS_SPPO_SELECTOR = DBTSelector(
    name="viagens_sppo",
    initial_datetime=VIAGENS_SPPO_INITIAL_DATETIME,
    final_datetime=VIAGENS_SPPO_FINAL_DATETIME,
    flow_folder_name="treatment__sppo_viagens",
    post_test=VIAGENS_SPPO_POST_TEST,
)

VIAGENS_SPPO_D0_SELECTOR = DBTSelector(
    name="viagens_sppo_d0",
    initial_datetime=VIAGENS_SPPO_INITIAL_DATETIME,
    final_datetime=VIAGENS_SPPO_FINAL_DATETIME,
    flow_folder_name="treatment__sppo_viagens",
)

VIAGENS_SPPO_SNAPSHOT_SELECTOR = DBTSelector(
    name="snapshot_viagem",
    initial_datetime=VIAGENS_SPPO_INITIAL_DATETIME,
    flow_folder_name="treatment__sppo_viagens",
)
