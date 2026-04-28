# -*- coding: utf-8 -*-
"""
Valores constantes compartilhados para captura de dados de GPS (cittati, conecta, zirix)
"""

REGISTROS_TABLE_ID = "registros"
REALOCACAO_TABLE_ID = "realocacao"

CITTATI_SOURCE_NAME = "cittati"
CONECTA_SOURCE_NAME = "conecta"
ZIRIX_SOURCE_NAME = "zirix"

GPS_SOURCE_CONFIGS = {
    "cittati": {
        "base_url": "https://servicos.cittati.com.br/WSIntegracaoCittati/SMTR/v2",
        "secret_path": "cittati_api",
        "registros_endpoint": "EnvioRastreamentos",
        "realocacao_endpoint": "EnvioViagensRetroativasSMTR",
        "date_format": "%Y-%m-%d %H:%M:%S",
    },
    "conecta": {
        "base_url": "https://ccomobility.com.br/webservices/binder/wsconecta",
        "secret_path": "conecta_api",
        "registros_endpoint": "envioSMTR",
        "realocacao_endpoint": "EnvioRealocacoesSMTR",
        "date_format": "%Y-%m-%d %H:%M:%S",
    },
    "zirix": {
        "base_url": "https://zxbus.zirix.app.br/v2",
        "secret_path": "zirix_api_v2",
        "registros_endpoint": "posicao",
        "realocacao_endpoint": "realocacao",
        "date_format": "%Y-%m-%dT%H:%M:%S",
    },
}
