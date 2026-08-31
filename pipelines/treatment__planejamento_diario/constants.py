# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector planejamento_diario.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

PLANEJAMENTO_DIARIO_CHECKS_LIST = {
    "test_consistencia_servico_planejado_faixa_horaria": {
        "description": (
            "Os totais de partidas e quilometragem dos serviços planejados "
            "correspondem aos da Ordem de Serviço para cada "
            "`feed_start_date`, `tipo_dia` e `tipo_os`."
        )
    },
    "viagem_planejada_planejamento_dia": {
        "dbt_utils__unique_combination_of_columns__viagem_planejada_planejamento_dia": {
            "description": "Todos os registros de 'data' e 'id_viagem' são únicos."
        },
    },
    "sumario_faixa_servico_dia": {
        "check_km_planejada__sumario_faixa_servico_dia": {
            "description": "Todas as viagens possuem `km_planejada` correspondente à OS"
        },
    },
    "sumario_faixa_servico_dia_pagamento": {
        "check_km_planejada__sumario_faixa_servico_dia_pagamento": {
            "description": "Todas as viagens possuem `km_planejada` correspondente à OS"
        },
    },
    "viagem_planejada": {
        "check_km_planejada": {
            "description": "Todas as viagens possuem `km_planejada` correspondente à OS"
        },
    },
    "viagens_remuneradas": {
        "teto_viagens__viagens_remuneradas": {
            "description": (
                "Todas as viagens foram corretamente identificadas dentro das regras de limite"
            )
        },
    },
}

PLANEJAMENTO_DIARIO_TEST = DBTTest(
    test_select="servico_planejado_faixa_horaria viagem_planejada_planejamento_dia",
    exclude="tag:freshness",
    test_descriptions=PLANEJAMENTO_DIARIO_CHECKS_LIST,
    truncate_date=True,
)

PLANEJAMENTO_DIARIO_SELECTOR = DBTSelector(
    name="planejamento_diario",
    initial_datetime=datetime(2024, 9, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__planejamento_diario",
    post_test=PLANEJAMENTO_DIARIO_TEST,
)

SNAPSHOT_PLANEJAMENTO_SELECTOR = DBTSelector(
    name="snapshot_planejamento",
    initial_datetime=datetime(2024, 9, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__planejamento_diario",
)
