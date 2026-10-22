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

RIOROTATIVO_ORDEM_PAGAMENTO_PRE_TEST = DBTTest(
    test_select=(
        "test_jae_captura_riorotativo "
        "ativacao_riorotativo "
        "verificacao_guardador_veiculo_riorotativo"
    ),
    truncate_date=True,
    delay_days_start=1,
)


RIOROTATIVO_ORDEM_PAGAMENTO_POST_TEST = DBTTest(
    test_select=(
        "ordem_pagamento_guardador_veiculo_dia_riorotativo ordem_pagamento_entidade_dia_riorotativo"
    ),
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
