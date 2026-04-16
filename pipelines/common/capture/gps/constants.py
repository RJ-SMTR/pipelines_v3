# -*- coding: utf-8 -*-
"""
Valores constantes compartilhados para captura de dados de GPS (cittati, conecta, zirix, sppo)
"""

REGISTROS_TABLE_ID = "registros"
REALOCACAO_TABLE_ID = "realocacao"

CITTATI_SOURCE_NAME = "cittati"
CONECTA_SOURCE_NAME = "conecta"
ZIRIX_SOURCE_NAME = "zirix"
SPPO_SOURCE_NAME = "sppo"

GPS_SOURCE_CONFIGS = {
    "cittati": {
        "base_url": "https://servicos.cittati.com.br/WSIntegracaoCittati/SMTR/v2",
        "secret_path": "cittati_api",
        "registros_endpoint": "EnvioRastreamentos",
        "realocacao_endpoint": "EnvioViagensRetroativasSMTR",
    },
    "conecta": {
        "base_url": "https://ccomobility.com.br/webservices/binder/wsconecta",
        "secret_path": "conecta_api",
        "registros_endpoint": "envioSMTR",
        "realocacao_endpoint": "EnvioRealocacoesSMTR",
    },
    "zirix": {
        "base_url": "https://integration.systemsatx.com.br/Globalbus/SMTR/V2",
        "secret_path": "zirix_api",
        "registros_endpoint": "EnvioIplan",
        "realocacao_endpoint": "EnvioViagensRetroativas",
    },
    "sppo": {
        "base_url": "http://ccomobility.com.br/WebServices/Binder/wsconecta",
        "registros_secret_path": "sppo_api_v2",
        "realocacao_secret_path": "realocacao_api",
        "registros_endpoint": "EnvioIplan",
        "realocacao_endpoint": "EnvioViagensRetroativasSMTR",
    },
}
