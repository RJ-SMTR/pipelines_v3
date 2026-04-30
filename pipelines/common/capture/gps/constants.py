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

OUTPUT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

SPPO_REGISTROS_RENAME = {
    "ordem": "id_veiculo",
    "linha": "servico",
    "datahora": "datetime",
    "datahoraenvio": "datetime_envio",
    "datahoraservidor": "datetime_servidor",
}

SPPO_REGISTROS_DATETIME_COLS = ["datetime", "datetime_envio", "datetime_servidor"]

SPPO_REALOCACAO_RENAME = {
    "veiculo": "id_veiculo",
    "dataOperacao": "datetime_operacao",
    "linha": "servico",
    "dataEntrada": "datetime_entrada",
    "dataSaida": "datetime_saida",
    "dataProcessado": "datetime_processamento",
}

SPPO_REALOCACAO_DATETIME_COLS = [
    "datetime_operacao",
    "datetime_entrada",
    "datetime_saida",
    "datetime_processamento",
]

REALOCACAO_DATETIME_INPUT_FORMATS = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]

GPS_SOURCE_CONFIGS = {
    "cittati": {
        "base_url": "https://servicos.cittati.com.br/WSIntegracaoCittati/SMTR/v2",
        "secret_path": "cittati_api",
        "registros_endpoint": "EnvioRastreamentos",
        "realocacao_endpoint": "EnvioViagensRetroativasSMTR",
        "registros_datetime_format": "%Y-%m-%d %H:%M:%S",
        "realocacao_datetime_format": "%Y-%m-%d %H:%M:%S",
    },
    "conecta": {
        "base_url": "https://ccomobility.com.br/webservices/binder/wsconecta",
        "secret_path": "conecta_api",
        "registros_endpoint": "envioSMTR",
        "realocacao_endpoint": "EnvioRealocacoesSMTR",
        "registros_datetime_format": "%Y-%m-%d %H:%M:%S",
        "realocacao_datetime_format": "%Y-%m-%d %H:%M:%S",
    },
    "zirix": {
        "base_url": "https://zxbus.zirix.app.br/v2",
        "secret_path": "zirix_api_v2",
        "registros_endpoint": "posicao",
        "realocacao_endpoint": "realocacao",
        "registros_datetime_format": "%Y-%m-%dT%H:%M:%S",
        "realocacao_datetime_format": "%Y-%m-%dT%H:%M:%S",
    },
    "sppo": {
        "base_url": "http://ccomobility.com.br/WebServices/Binder/wsconecta",
        "registros_secret_path": "sppo_api_v2",
        "realocacao_secret_path": "realocacao_api",
        "registros_endpoint": "EnvioIplan",
        "realocacao_endpoint": "EnvioViagensRetroativasSMTR",
        "registros_datetime_format": "%Y-%m-%d+%H:%M:%S",
        "realocacao_datetime_format": "%Y-%m-%dT%H:%M:%S",
        "timezone": "America/Sao_Paulo",
    },
}
