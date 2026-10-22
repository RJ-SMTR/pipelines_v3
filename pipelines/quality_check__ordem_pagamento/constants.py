# -*- coding: utf-8 -*-
"""
Valores constantes para o teste de qualidade da ordem de pagamento da Jaé
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import DBTTest

ORDEM_PAGAMENTO_ALERT_WEBHOOK = "alertas_bilhetagem_ordem_pagamento"


ORDEM_PAGAMENTO_DBT_TEST = DBTTest(
    test_select="financeiro.bilhetagem_consorcio_operador_dia",
    exclude="transacao_valor_ordem_completa__transacao_valor_ordem",
    test_start_datetime=datetime(2026, 3, 25, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    test_alias="financeiro.bilhetagem_consorcio_operador_dia",
)
