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
        "dbt_utils.unique_combination_of_columns__viagem_planejada": {
            "description": (
                "Todos os registros de viagem_planejada são únicos na chave do "
                "snapshot_viagem_planejada (data, servico, sentido, faixa_horaria_inicio, "
                "trip_id, trip_id_planejado, shape_id, shape_id_planejado)."
            )
        },
    },
}

VIAGENS_SPPO_POST_TEST = DBTTest(
    test_select="dbt_utils.unique_combination_of_columns__viagem_planejada",
    test_descriptions=VIAGENS_SPPO_CHECKS_LIST,
    truncate_date=True,
)

VIAGENS_SPPO_SELECTOR = DBTSelector(
    name="viagens_sppo",
    initial_datetime=datetime(2026, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__sppo_viagens",
    post_test=VIAGENS_SPPO_POST_TEST,
)

VIAGENS_SPPO_D0_SELECTOR = DBTSelector(
    name="viagens_sppo_d0",
    initial_datetime=datetime(2026, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__sppo_viagens",
)

VIAGENS_SPPO_SNAPSHOT_SELECTOR = DBTSelector(
    name="snapshot_viagem",
    initial_datetime=datetime(2026, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__sppo_viagens",
)
