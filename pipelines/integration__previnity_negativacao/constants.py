# -*- coding: utf-8 -*-
"""
Valores constantes para pipeline integration__previnity_negativacao
"""

API_URL_PF = "https://api.previnity.com.br/pnn035"
API_URL_PJ = "https://api.previnity.com.br/pnn034"

HEADER_PREV_KEY = "PREVKEY"
HEADER_PREV_TOKEN = "PREVTOKEN"
HEADER_CONTENT_TYPE = "Content-Type"

SECRET_PATH = "previnity_api"

QUERY_PF = "SELECT * FROM `rj-smtr.transito_interno.view_pessoa_fisica_negativacao`"
QUERY_PJ = "SELECT * FROM `rj-smtr.transito_interno.view_pessoa_juridica_negativacao`"
