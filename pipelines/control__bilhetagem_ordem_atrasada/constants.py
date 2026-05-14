# -*- coding: utf-8 -*-
"""
Valores constantes para os processos manuais de bilhetagem
"""

from pipelines.capture.jae.constants import CLIENTE_TABLE_ID
from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.capture.jae.flows import (
    CAPTURA_AUXILIAR,
    CAPTURA_GPS_VALIDADOR,
    CAPTURA_LANCAMENTO,
    CAPTURA_TRANSACAO,
    CAPTURA_TRANSACAO_RIOCARD,
)
from pipelines.treatment.bilhetagem.constants import constants as bilhetagem_constants
from pipelines.treatment.bilhetagem.flows import (
    EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO,
    TRANSACAO_MATERIALIZACAO,
)
from pipelines.treatment.cadastro.constants import constants as cadastro_constants
from pipelines.treatment.cadastro.flows import CADASTRO_MATERIALIZACAO
from pipelines.treatment.monitoramento.constants import (
    constants as monitoramento_constants,
)
from pipelines.treatment.monitoramento.flows import GPS_VALIDADOR_MATERIALIZACAO

CAPTURE_GAP_TABLES = {
        jae_constants.TRANSACAO_TABLE_ID: {
            "flow_name": CAPTURA_TRANSACAO,
            "reprocess_all": False,
        },
        jae_constants.TRANSACAO_RIOCARD_TABLE_ID: {
            "flow_name": CAPTURA_TRANSACAO_RIOCARD,
            "reprocess_all": False,
        },
        jae_constants.GPS_VALIDADOR_TABLE_ID: {
            "flow_name": CAPTURA_GPS_VALIDADOR,
            "reprocess_all": False,
        },
        jae_constants.LANCAMENTO_TABLE_ID: {
            "flow_name": CAPTURA_LANCAMENTO,
            "reprocess_all": False,
        },
        CLIENTE_TABLE_ID: {"flow_name": CAPTURA_AUXILIAR, "reprocess_all": True},
    },

CAPTURE_GAP_SELECTORS = {
        cadastro_constants.CADASTRO_SELECTOR: {
            "flow_name": CADASTRO_MATERIALIZACAO,
            "capture_tables": [CLIENTE_TABLE_ID],
            "selector": cadastro_constants.CADASTRO_SELECTOR,
        },
        bilhetagem_constants.TRANSACAO_SELECTOR: {
            "flow_name": TRANSACAO_MATERIALIZACAO,
            "capture_tables": [
                jae_constants.TRANSACAO_TABLE_ID,
                jae_constants.TRANSACAO_RIOCARD_TABLE_ID,
                CLIENTE_TABLE_ID,
            ],
            "selector": bilhetagem_constants.TRANSACAO_SELECTOR,
        },
        monitoramento_constants.GPS_VALIDADOR_SELECTOR: {
            "flow_name": GPS_VALIDADOR_MATERIALIZACAO,
            "capture_tables": [jae_constants.GPS_VALIDADOR_TABLE_ID],
            "selector": monitoramento_constants.GPS_VALIDADOR_SELECTOR,
        },
        bilhetagem_constants.EXTRATO_CLIENTE_CARTAO_SELECTOR: {
            "flow_name": EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO,
            "capture_tables": [jae_constants.LANCAMENTO_TABLE_ID, CLIENTE_TABLE_ID],
            "selector": bilhetagem_constants.EXTRATO_CLIENTE_CARTAO_SELECTOR,
        },
    },
