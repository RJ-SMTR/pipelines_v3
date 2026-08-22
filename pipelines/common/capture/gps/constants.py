# -*- coding: utf-8 -*-
"""
Valores constantes compartilhados para captura de dados de GPS
"""

REGISTROS_TABLE_ID = "registros"
REALOCACAO_TABLE_ID = "realocacao"

CITTATI_SOURCE_NAME = "cittati"
CONECTA_SOURCE_NAME = "conecta"
ZIRIX_SOURCE_NAME = "zirix"
SPPO_SOURCE_NAME = "sppo"
SONDA_SOURCE_NAME = "sonda"
MAXTRACK_SOURCE_NAME = "maxtrack"

OUTPUT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

SONDA_REGISTROS_RENAME = {
    "codigo": "id_veiculo",
    "linha": "servico",
    "latitude": "latitude",
    "longitude": "longitude",
    "dataHora": "datetime",
    "velocidade": "velocidade",
    "sentido": "sentido",
    "trajeto": "vista",
}

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
    SONDA_SOURCE_NAME: {
        "base_url": "https://zn4.m2mcontrol.com.br/api/integracao/veiculos",
        "auth": {
            "secret_path": "brt_api_v2",
            "credentials_as_headers": True,
        },
        "requests": {
            REGISTROS_TABLE_ID: {"response_key": "veiculos"},
        },
    },
    CITTATI_SOURCE_NAME: {
        "base_url": "https://servicos.cittati.com.br/WSIntegracaoCittati/SMTR/v2",
        "auth": {
            "secret_path": "cittati_api",
            "parameter": "guidIdentificacao",
        },
        "requests": {
            REGISTROS_TABLE_ID: {
                "endpoint": "EnvioRastreamentos",
                "window": {
                    "datetime_format": "%Y-%m-%d %H:%M:%S",
                    "start_offset_minutes": 6,
                    "end_offset_minutes": 5,
                },
            },
            REALOCACAO_TABLE_ID: {
                "endpoint": "EnvioViagensRetroativasSMTR",
                "window": {
                    "datetime_format": "%Y-%m-%d %H:%M:%S",
                    "start_offset_minutes": 10,
                    "end_offset_minutes": 0,
                },
            },
        },
    },
    CONECTA_SOURCE_NAME: {
        "base_url": "https://ccomobility.com.br/webservices/binder/wsconecta",
        "auth": {
            "secret_path": "conecta_api",
            "parameter": "guidIdentificacao",
        },
        "requests": {
            REGISTROS_TABLE_ID: {
                "endpoint": "envioSMTR",
                "window": {
                    "datetime_format": "%Y-%m-%d %H:%M:%S",
                    "start_offset_minutes": 6,
                    "end_offset_minutes": 5,
                },
            },
            REALOCACAO_TABLE_ID: {
                "endpoint": "EnvioRealocacoesSMTR",
                "window": {
                    "datetime_format": "%Y-%m-%d %H:%M:%S",
                    "start_offset_minutes": 10,
                    "end_offset_minutes": 0,
                },
            },
        },
    },
    ZIRIX_SOURCE_NAME: {
        "base_url": "https://zxbus.zirix.app.br/v2",
        "auth": {
            "secret_path": "zirix_api_v2",
            "parameter": "guidIdentificacao",
        },
        "requests": {
            REGISTROS_TABLE_ID: {
                "endpoint": "posicao",
                "window": {
                    "datetime_format": "%Y-%m-%dT%H:%M:%S",
                    "start_offset_minutes": 6,
                    "end_offset_minutes": 5,
                },
            },
            REALOCACAO_TABLE_ID: {
                "endpoint": "realocacao",
                "window": {
                    "datetime_format": "%Y-%m-%dT%H:%M:%S",
                    "start_offset_minutes": 10,
                    "end_offset_minutes": 0,
                },
            },
        },
    },
    SPPO_SOURCE_NAME: {
        "base_url": "https://ccomobility.com.br/WebServices/Binder/wsconecta",
        "requests": {
            REGISTROS_TABLE_ID: {
                "endpoint": "EnvioIplan",
                "auth": {
                    "secret_path": "sppo_api_v2",
                    "parameter": "guidIdentificacao",
                },
                "window": {
                    "datetime_format": "%Y-%m-%d %H:%M:%S",
                    "start_offset_minutes": 6,
                    "end_offset_minutes": 5,
                    "timezone": "America/Sao_Paulo",
                },
            },
            REALOCACAO_TABLE_ID: {
                "endpoint": "EnvioViagensRetroativasSMTR",
                "auth": {
                    "secret_path": "realocacao_api",
                    "parameter": "guidIdentificacao",
                },
                "window": {
                    "datetime_format": "%Y-%m-%dT%H:%M:%S",
                    "start_offset_minutes": 10,
                    "end_offset_minutes": 0,
                    "timezone": "America/Sao_Paulo",
                },
            },
        },
    },
    MAXTRACK_SOURCE_NAME: {
        "base_url": "https://api-sspo-rj.maxtrack.com.br/api/v1",
        "auth": {
            "secret_path": "maxtrack_api",
            "secret_name": "token",
            "header": "Authorization",
            "scheme": "Bearer",
        },
        "requests": {
            REGISTROS_TABLE_ID: {
                "endpoint": "posicoes",
                "window": {
                    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                    "start_offset_minutes": 6,
                    "end_offset_minutes": 5,
                    "start_parameter": "datetime_servidor_inicio",
                    "end_parameter": "datetime_servidor_fim",
                },
            },
        },
    },
}
