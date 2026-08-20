# -*- coding: utf-8 -*-
"""
Constantes para o flow de verificação da captura dos dados da Jaé
"""

from pipelines.capture__jae_auxiliar import constants as auxiliar_constants
from pipelines.capture__jae_estacionamento_veiculo import (
    constants as estacionamento_veiculo_constants,
)
from pipelines.capture__jae_fiscalizacao_veiculo import constants as fiscalizacao_veiculo_constants
from pipelines.capture__jae_gps_validador import constants as gps_validador_constants
from pipelines.capture__jae_lancamento import constants as lancamento_constants
from pipelines.capture__jae_movimento_estacionamento_veiculo import (
    constants as movimento_estacionamento_veiculo_constants,
)
from pipelines.capture__jae_riorotativo_auxiliar import constants as riorotativo_auxiliar_constants
from pipelines.capture__jae_transacao import constants as transacao_constants
from pipelines.capture__jae_transacao_erro import constants as transacao_erro_constants
from pipelines.capture__jae_transacao_riocard import constants as transacao_riocard_constants
from pipelines.common.capture.jae import constants as jae_constants

RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID = "resultado_verificacao_captura_jae"

CLIENTE_SOURCE = next(
    s
    for s in auxiliar_constants.JAE_AUXILIAR_SOURCES
    if s.table_id == jae_constants.CLIENTE_TABLE_ID
)

GRATUIDADE_SOURCE = next(
    s
    for s in auxiliar_constants.JAE_AUXILIAR_SOURCES
    if s.table_id == jae_constants.GRATUIDADE_TABLE_ID
)

ESTUDANTE_SOURCE = next(
    s
    for s in auxiliar_constants.JAE_AUXILIAR_SOURCES
    if s.table_id == jae_constants.ESTUDANTE_TABLE_ID
)

LAUDO_PCD_SOURCE = next(
    s
    for s in auxiliar_constants.JAE_AUXILIAR_SOURCES
    if s.table_id == jae_constants.LAUDO_PCD_TABLE_ID
)

VEICULO_SOURCE = next(
    s
    for s in riorotativo_auxiliar_constants.JAE_RIOROTATIVO_AUXILIAR_SOURCES
    if s.table_id == jae_constants.VEICULO_TABLE_ID
)


CHECK_CAPTURE_PARAMS = {
    jae_constants.TRANSACAO_TABLE_ID: {
        "source": transacao_constants.TRANSACAO_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_staging.transacao",
        "timestamp_column": "data_processamento",
        "primary_keys": transacao_constants.TRANSACAO_SOURCE.primary_keys,
        "final_timestamp_exclusive": True,
    },
    jae_constants.TRANSACAO_RIOCARD_TABLE_ID: {
        "source": transacao_riocard_constants.TRANSACAO_RIOCARD_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_staging.transacao_riocard",
        "timestamp_column": "data_processamento",
        "primary_keys": transacao_riocard_constants.TRANSACAO_RIOCARD_SOURCE.primary_keys,
        "final_timestamp_exclusive": True,
    },
    jae_constants.GPS_VALIDADOR_TABLE_ID: {
        "source": gps_validador_constants.GPS_VALIDADOR_SOURCE,
        "datalake_table": "rj-smtr.monitoramento_staging.gps_validador",
        "timestamp_column": "data_tracking",
        "primary_keys": gps_validador_constants.GPS_VALIDADOR_SOURCE.primary_keys,
        "final_timestamp_exclusive": True,
    },
    jae_constants.LANCAMENTO_TABLE_ID: {
        "source": lancamento_constants.LANCAMENTO_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_interno_staging.lancamento",
        "timestamp_column": "dt_lancamento",
        "primary_keys": [
            "ifnull(id_lancamento, concat(string(dt_lancamento), '_', id_movimento))",
            "id_conta",
        ],
        "final_timestamp_exclusive": True,
    },
    jae_constants.TRANSACAO_ERRO_TABLE_ID: {
        "source": transacao_erro_constants.JAE_TRANSACAO_ERRO_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_interno_staging.transacao_erro",
        "timestamp_column": "dt_inclusao",
        "primary_keys": transacao_erro_constants.JAE_TRANSACAO_ERRO_SOURCE.primary_keys,
        "final_timestamp_exclusive": True,
    },
    jae_constants.CLIENTE_TABLE_ID: {
        "source": CLIENTE_SOURCE,
        "datalake_table": "rj-smtr.cadastro_interno_staging.cliente",
        "timestamp_column": "dt_cadastro",
        "primary_keys": CLIENTE_SOURCE.primary_keys,
        "final_timestamp_exclusive": False,
    },
    jae_constants.GRATUIDADE_TABLE_ID: {
        "source": GRATUIDADE_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_staging.gratuidade",
        "timestamp_column": "data_inclusao",
        "primary_keys": GRATUIDADE_SOURCE.primary_keys,
        "final_timestamp_exclusive": False,
    },
    jae_constants.ESTUDANTE_TABLE_ID: {
        "source": ESTUDANTE_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_staging.estudante",
        "timestamp_column": "data_inclusao",
        "primary_keys": ["cd_cliente", "data_inclusao"],
        "final_timestamp_exclusive": False,
    },
    jae_constants.LAUDO_PCD_TABLE_ID: {
        "source": LAUDO_PCD_SOURCE,
        "datalake_table": "rj-smtr.bilhetagem_staging.laudo_pcd",
        "timestamp_column": "data_inclusao",
        "primary_keys": LAUDO_PCD_SOURCE.primary_keys,
        "final_timestamp_exclusive": False,
    },
    jae_constants.VEICULO_TABLE_ID: {
        "source": VEICULO_SOURCE,
        "datalake_table": "rj-smtr.riorotativo_interno_staging.veiculo",
        "timestamp_column": "data_inclusao",
        "primary_keys": ["id_veiculo"],
        "final_timestamp_exclusive": True,
        "datalake_timestamp_captura_column": "datetime_captura",
    },
    jae_constants.MOVIMENTO_ESTACIONAMENTO_VEICULO_TABLE_ID: {
        "source": movimento_estacionamento_veiculo_constants.MOVIMENTO_ESTACIONAMENTO_VEICULO_SOURCE,
        "datalake_table": "rj-smtr.riorotativo_staging.movimento_estacionamento_veiculo",
        "timestamp_column": "data_inclusao",
        "primary_keys": ["id_movimento_estacionamento_veiculo"],
        "final_timestamp_exclusive": True,
        "datalake_timestamp_captura_column": "datetime_captura",
    },
    jae_constants.ESTACIONAMENTO_VEICULO_TABLE_ID: {
        "source": estacionamento_veiculo_constants.ESTACIONAMENTO_VEICULO_SOURCE,
        "datalake_table": "rj-smtr.riorotativo_staging.estacionamento_veiculo",
        "timestamp_column": "data_inclusao",
        "primary_keys": ["id_estacionamento_veiculo"],
        "final_timestamp_exclusive": True,
        "datalake_timestamp_captura_column": "datetime_captura",
    },
    jae_constants.FISCALIZACAO_VEICULO_TABLE_ID: {
        "source": fiscalizacao_veiculo_constants.FISCALIZACAO_VEICULO_SOURCE,
        "datalake_table": "rj-smtr.riorotativo_interno_staging.fiscalizacao_veiculo",
        "timestamp_column": "data_inclusao",
        "primary_keys": ["id_fiscalizacao_veiculo"],
        "final_timestamp_exclusive": True,
        "datalake_timestamp_captura_column": "datetime_captura",
    },
}
