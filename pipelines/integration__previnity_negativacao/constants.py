# -*- coding: utf-8 -*-
"""
Valores constantes para pipeline integration__previnity_negativacao
"""

API_URL_PF = "https://api.previnity.com.br/pnn035"
API_URL_PJ = "https://api.previnity.com.br/pnn034"

SECRET_PATH = "previnity_api"

QUERY_PF = "SELECT * FROM `rj-smtr-dev.botelho__transito.view_pessoa_fisica_negativacao` WHERE data = '2024-12-31' LIMIT 100"
QUERY_PJ = "SELECT * FROM `rj-smtr.transito_interno.view_pessoa_juridica_negativacao`"
