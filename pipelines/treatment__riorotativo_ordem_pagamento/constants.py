# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos selectors dos dados das ordens de pagamento do Rio Rotativo Digital.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTSelector, DBTTest
from pipelines.treatment__riorotativo_auxiliar import constants as riorotativo_auxiliar_constants
from pipelines.treatment__riorotativo_operacao import constants as riorotativo_operacao_constants

RIOROTATIVO_ORDEM_PAGAMENTO_PRE_TEST_DESCRIPTIONS = {
    "test_jae_captura_riorotativo": {"description": ("Captura da Jaé sem timestamps ausentes")},
    "ativacao_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
    },
    "verificacao_guardador_veiculo_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
    },
}

RIOROTATIVO_ORDEM_PAGAMENTO_PRE_TEST = DBTTest(
    test_select=(
        "test_jae_captura_riorotativo "
        "ativacao_riorotativo "
        "verificacao_guardador_veiculo_riorotativo"
    ),
    test_descriptions=RIOROTATIVO_ORDEM_PAGAMENTO_PRE_TEST_DESCRIPTIONS,
    truncate_date=True,
    delay_days_start=1,
)


RIOROTATIVO_ORDEM_PAGAMENTO_POST_TEST_DESCRIPTIONS = {
    "ordem_pagamento_guardador_veiculo_dia_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
        "dbt_utils__unique_combination_of_columns__data_cpf__ordem_pagamento_guardador_veiculo_dia_riorotativo": {
            "description": "Todos os dados de 'data_ordem' e 'cpf_guardador_veiculo' são únicos."
        },
    },
    "ordem_pagamento_entidade_dia_riorotativo": {
        "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        "unique": {"description": "Todos os registros são únicos"},
        "dbt_utils__unique_combination_of_columns__data_cnpj__ordem_pagamento_entidade_dia_riorotativo": {
            "description": "Todos os dados de 'data_ordem' e 'cnpj_entidade' são únicos."
        },
    },
}

RIOROTATIVO_ORDEM_PAGAMENTO_POST_TEST = DBTTest(
    test_select=(
        "ordem_pagamento_guardador_veiculo_dia_riorotativo ordem_pagamento_entidade_dia_riorotativo"
    ),
    test_descriptions=RIOROTATIVO_ORDEM_PAGAMENTO_POST_TEST_DESCRIPTIONS,
    truncate_date=True,
)

RIOROTATIVO_ORDEM_PAGAMENTO_SELECTOR = DBTSelector(
    name="riorotativo_ordem_pagamento",
    initial_datetime=datetime(2026, 7, 30, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo_ordem_pagamento",
    data_sources=[
        riorotativo_auxiliar_constants.RIOROTATIVO_AUX_SELECTOR,
        riorotativo_operacao_constants.RIOROTATIVO_OPERACAO_SELECTOR,
    ],
    pre_test=RIOROTATIVO_ORDEM_PAGAMENTO_PRE_TEST,
    post_test=RIOROTATIVO_ORDEM_PAGAMENTO_POST_TEST,
)

RIOROTATIVO_ORDEM_PAGAMENTO_SNAPSHOT = DBTSelector(
    name="snapshot_riorotativo_ordem_pagamento",
    initial_datetime=datetime(2026, 7, 30, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__riorotativo_ordem_pagamento",
)
