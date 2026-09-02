# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de viagem informada
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.capture__maxtrack_viagem_informada import constants as maxtrack_constants
from pipelines.capture__rioonibus_viagem_informada import constants as rioonibus_constants
from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__planejamento_diario import constants as planejamento_constants

VIAGEM_INFORMADA_CHECKS_LIST = {
    "viagem_informada_monitoramento": {
        "dbt_utils__unique_combination_of_columns__data_id_viagem_planejada__viagem_informada_monitoramento": {  # noqa: E501
            "description": ("Cada viagem planejada possui no máximo uma viagem realizada por data.")
        },
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os valores da coluna `{column_name}` são únicos"},
        "dbt_utils__sequential_values__sequencial_viagem__viagem_informada_monitoramento": {
            "description": "Os valores de `sequencial_viagem` são contínuos, sem buracos, no intervalo testado."  # noqa: E501
        },
        "dbt_utils__relationships_where__data_id_viagem_planejada__viagem_informada_monitoramento": {  # noqa: E501
            "description": "Cada par `(data, id_viagem_planejada)` consta em `viagem_planejada_dia`.",  # noqa: E501
        },
        "dbt_utils__relationships_where__route_id__viagem_informada_monitoramento": {
            "description": "Todo `route_id` informado corresponde a um `route_id` do GTFS associado ao período testado.",  # noqa: E501
        },
        "dbt_utils__relationships_where__trip_id__viagem_informada_monitoramento": {
            "description": "Todo `trip_id` informado corresponde a um `trip_id` do GTFS associado ao período testado.",  # noqa: E501
        },
        "dbt_utils__relationships_where__shape_id__viagem_informada_monitoramento": {
            "description": "Todo `shape_id` informado corresponde a um `shape_id` do GTFS associado ao período testado.",  # noqa: E501
        },
    }
}

VIAGEM_INFORMADA_TEST = DBTTest(
    test_select="viagem_informada_monitoramento",
    test_descriptions=VIAGEM_INFORMADA_CHECKS_LIST,
    truncate_date=True,
)

VIAGEM_INFORMADA_SELECTOR = DBTSelector(
    name="viagem_informada",
    initial_datetime=datetime(2024, 10, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__viagem_informada",
    data_sources=[
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR,
        rioonibus_constants.VIAGEM_INFORMADA_SOURCE,
        maxtrack_constants.VIAGEM_INFORMADA_SOURCE,
    ],
    post_test=VIAGEM_INFORMADA_TEST,
)
