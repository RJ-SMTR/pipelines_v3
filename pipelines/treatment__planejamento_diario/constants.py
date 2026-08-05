# -*- coding: utf-8 -*-
"""
Valores constantes para materialização do selector planejamento_diario.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest

PLANEJAMENTO_DIARIO_CHECKS_LIST = {
    "viagem_planejada_planejamento_dia": {
        "dbt_utils.unique_combination_of_columns__viagem_planejada_planejamento_dia": {
            "description": "Todos os registros de 'data' e 'id_viagem' são únicos."
        },
        "dbt_expectations.expect_table_aggregation_to_equal_other_table__trajetos_alternativos__viagem_planejada_planejamento_dia": {
            "description": (
                "A quantidade distinta de eventos de trajetos alternativos corresponde "
                "entre a Ordem de Serviço e `viagem_planejada_planejamento_dia` para o "
                "mesmo `feed_start_date`, `servico`, `tipo_os` e `sentido`, considerando "
                "todas as combinações da Ordem de Serviço dos feeds presentes na janela "
                "diária, inclusive as ausentes no planejamento."
            )
        },
        "planejamento_gtfs_freshness__viagem_planejada_planejamento_dia": {
            "description": (
                "Para cada feed GTFS com `feed_update_datetime` na janela, as partições "
                "diárias da vigência em `viagem_planejada_planejamento_dia` existem, "
                "contêm o `feed_start_date` esperado e foram gravadas após a "
                "atualização do feed."
            )
        },
    },
}

PLANEJAMENTO_DIARIO_TEST = DBTTest(
    test_select="viagem_planejada_planejamento_dia",
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
