# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines import constants as smtr_constants
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.pretreatment import raise_if_column_isna

JAE_SOURCE_NAME = "jae"

JAE_DATABASE_SETTINGS = {
    "principal_db": {
        "engine": "mysql",
        "host": "10.5.113.238",
    },
    "tarifa_db": {
        "engine": "postgresql",
        "host": "10.5.113.254",
    },
    "transacao_db": {
        "engine": "postgresql",
        "host": "10.5.114.104",
    },
    "tracking_db": {
        "engine": "postgresql",
        "host": "10.5.14.235",
    },
    "ressarcimento_db": {
        "engine": "postgresql",
        "host": "10.5.12.50",
    },
    "gratuidade_db": {
        "engine": "postgresql",
        "host": "10.5.14.19",
    },
    "fiscalizacao_db": {
        "engine": "postgresql",
        "host": "10.5.115.29",
    },
    "atm_gateway_db": {
        "engine": "postgresql",
        "host": "10.5.15.127",
    },
    "device_db": {
        "engine": "postgresql",
        "host": "10.5.114.114",
    },
    "erp_integracao_db": {
        "engine": "postgresql",
        "host": "10.5.12.105",
    },
    "financeiro_db": {
        "engine": "postgresql",
        "host": "10.5.12.109",
    },
    "midia_db": {
        "engine": "postgresql",
        "host": "10.5.12.52",
    },
    "processador_transacao_db": {
        "engine": "postgresql",
        "host": "10.5.14.59",
    },
    "atendimento_db": {
        "engine": "postgresql",
        "host": "10.5.14.170",
    },
    "gateway_pagamento_db": {
        "engine": "postgresql",
        "host": "10.5.113.130",
    },
    # "iam_db": {
    #     "engine": "mysql",
    #     "host": "10.5.13.201",
    # },
    "vendas_db": {
        "engine": "postgresql",
        "host": "10.5.114.15",
    },
}

JAE_SECRET_PATH = "smtr_jae_access_data"
JAE_PRIVATE_BUCKET_NAMES = {
    "prod": "rj-smtr-jae-private",
    "dev": "rj-smtr-dev-private",
}
ALERT_WEBHOOK = "alertas_bilhetagem"
RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID = "resultado_verificacao_captura_jae"

TRANSACAO_TABLE_ID = "transacao"
TRANSACAO_RIOCARD_TABLE_ID = "transacao_riocard"
GPS_VALIDADOR_TABLE_ID = "gps_validador"
INTEGRACAO_TABLE_ID = "integracao_transacao"
TRANSACAO_ORDEM_TABLE_ID = "transacao_ordem"
TRANSACAO_RETIFICADA_TABLE_ID = "transacao_retificada"
LANCAMENTO_TABLE_ID = "lancamento"
CLIENTE_TABLE_ID = "cliente"
GRATUIDADE_TABLE_ID = "gratuidade"
ESTUDANTE_TABLE_ID = "estudante"
LAUDO_PCD_TABLE_ID = "laudo_pcd"

JAE_TABLE_CAPTURE_PARAMS = {
    TRANSACAO_TABLE_ID: {
        "query": """
            SELECT
                *
            FROM
                transacao
            WHERE
                data_processamento >= timestamp '{start}' - INTERVAL '{delay} minutes'
                AND data_processamento < timestamp '{end}' - INTERVAL '{delay} minutes'
        """,
        "database": "transacao_db",
        "capture_delay_minutes": {"0": 0, "2025-03-26 15:36:00": 5},
    },
    TRANSACAO_RETIFICADA_TABLE_ID: {
        "query": """
            SELECT
                r.*,
                t.data_transacao
            FROM
                transacao_retificada r
            JOIN transacao t on r.id_transacao = t.id
            WHERE
                data_retificacao >= timestamp '{start}' - INTERVAL '5 minutes'
                AND data_retificacao < timestamp '{end}' - INTERVAL '5 minutes'
        """,
        "database": "transacao_db",
    },
    TRANSACAO_RIOCARD_TABLE_ID: {
        "query": """
            SELECT
                *
            FROM
                transacao_riocard
            WHERE
                data_processamento >= timestamp '{start}' - INTERVAL '{delay} minutes'
                AND data_processamento < timestamp '{end}' - INTERVAL '{delay} minutes'
        """,
        "database": "transacao_db",
        "capture_delay_minutes": {"0": 0, "2025-03-26 15:36:00": 5},
    },
    GPS_VALIDADOR_TABLE_ID: {
        "query": """
            SELECT
                *
            FROM
                tracking_detalhe
            WHERE
                data_tracking >= timestamp '{start}' - INTERVAL '{delay} minutes'
                AND data_tracking < timestamp '{end}' - INTERVAL '{delay} minutes'
        """,
        "database": "tracking_db",
        "capture_delay_minutes": {"0": 0, "2025-03-26 15:31:00": 10},
    },
    INTEGRACAO_TABLE_ID: {
        "database": "ressarcimento_db",
        "query": """
            SELECT
                *
            FROM
                integracao_transacao
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
            ORDER BY data_inclusao
        """,
    },
    TRANSACAO_ORDEM_TABLE_ID: {
        "query": """
            SELECT
                id,
                id_ordem_ressarcimento,
                data_processamento,
                data_transacao
            FROM
                transacao
            WHERE
                data_processamento >= '{start}'
                AND data_processamento <= '{end}'
                AND id_ordem_ressarcimento IS NOT NULL
            ORDER BY data_processamento
        """,
        "database": "transacao_db",
    },
    LANCAMENTO_TABLE_ID: {
        "query": """
            SELECT
                l.*,
                m.cd_tipo_movimento,
                tm.ds_tipo_movimento,
                tc.ds_tipo_conta,
                tc.id_tipo_moeda,
                tmo.descricao as tipo_moeda,
                c.cd_cliente,
                c.nr_logico_midia
            FROM
                lancamento l
            LEFT JOIN
                movimento m
            USING(id_movimento)
            LEFT JOIN
                tipo_movimento tm
            USING(cd_tipo_movimento)
            LEFT JOIN
                conta c
            USING(id_conta)
            LEFT JOIN
                tipo_conta tc
            USING(cd_tipo_conta)
            LEFT JOIN
                tipo_moeda tmo
            ON tc.id_tipo_moeda = tmo.id
            WHERE
                l.dt_lancamento >= timestamp '{start}' - INTERVAL '{delay} minutes'
                AND l.dt_lancamento < timestamp '{end}' - INTERVAL '{delay} minutes'
            ORDER BY l.dt_lancamento DESC

        """,
        "database": "financeiro_db",
        "capture_delay_minutes": {"0": 5},
    },
    "linha": {
        "query": """
            SELECT
                *
            FROM
                LINHA
        """,
        "database": "principal_db",
        "primary_keys": ["CD_LINHA"],
        "capture_flow": "auxiliar",
    },
    "produto": {
        "query": """
            SELECT
                *
            FROM
                PRODUTO
            WHERE
                DT_INCLUSAO BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "principal_db",
        "primary_keys": ["CD_PRODUTO"],
        "capture_flow": "auxiliar",
    },
    "operadora_transporte": {
        "query": """
            SELECT
                o.*,
                m.DS_TIPO_MODAL
            FROM
                OPERADORA_TRANSPORTE o
            LEFT JOIN
                TIPO_MODAL m
            ON
                o.CD_TIPO_MODAL = m.CD_TIPO_MODAL
            WHERE
                DT_INCLUSAO BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "principal_db",
        "primary_keys": ["CD_OPERADORA_TRANSPORTE"],
        "capture_flow": "auxiliar",
    },
    CLIENTE_TABLE_ID: {
        "query": """
            SELECT
                c.*
            FROM
                CLIENTE c
            WHERE
                DT_CADASTRO BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "principal_db",
        "primary_keys": ["CD_CLIENTE"],
        "pre_treatment_reader_args": {"dtype": {"NR_DOCUMENTO": "object"}},
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "capture_flow": "auxiliar",
    },
    "pessoa_fisica": {
        "query": """
            SELECT
                p.*,
                c.DT_CADASTRO
            FROM
                PESSOA_FISICA p
            JOIN
                CLIENTE c
            ON
                p.CD_CLIENTE = c.CD_CLIENTE
            WHERE
                c.DT_CADASTRO BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "principal_db",
        "primary_keys": ["CD_CLIENTE"],
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "capture_flow": "auxiliar",
    },
    GRATUIDADE_TABLE_ID: {
        "query": """
            SELECT
                g.*,
                t.descricao AS tipo_gratuidade
            FROM
                gratuidade g
            LEFT JOIN
                tipo_gratuidade t
            ON
                g.id_tipo_gratuidade = t.id
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "gratuidade_db",
        "primary_keys": ["id"],
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "capture_flow": "auxiliar",
    },
    "consorcio": {
        "query": """
            SELECT
                *
            FROM
                CONSORCIO
            WHERE
                DT_INCLUSAO BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "principal_db",
        "primary_keys": ["CD_CONSORCIO"],
        "capture_flow": "auxiliar",
    },
    "percentual_rateio_integracao": {
        "query": """
            SELECT
                *
            FROM
                percentual_rateio_integracao
            WHERE
                dt_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id"],
        "capture_flow": "auxiliar",
    },
    "linha_tarifa": {
        "query": """
            SELECT
                *
            FROM
                linha_tarifa
            WHERE
                dt_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "tarifa_db",
        "primary_keys": [
            "cd_linha",
            "nr_sequencia",
        ],
        "capture_flow": "auxiliar",
    },
    "linha_consorcio": {
        "query": """
            SELECT
                *
            FROM
                LINHA_CONSORCIO
            WHERE
                DT_INCLUSAO BETWEEN '{start}'
                AND '{end}'
                OR DT_FIM_VALIDADE BETWEEN DATE('{start}')
                AND DATE('{end}')
        """,
        "database": "principal_db",
        "primary_keys": [
            "CD_CONSORCIO",
            "CD_LINHA",
        ],
        "capture_flow": "auxiliar",
    },
    "linha_consorcio_operadora_transporte": {
        "query": """
            SELECT
                *
            FROM
                LINHA_CONSORCIO_OPERADORA_TRANSPORTE
            WHERE
                DT_INCLUSAO BETWEEN '{start}'
                AND '{end}'
                OR DT_FIM_VALIDADE BETWEEN DATE('{start}')
                AND DATE('{end}')
        """,
        "database": "principal_db",
        "primary_keys": [
            "CD_CONSORCIO",
            "CD_OPERADORA_TRANSPORTE",
            "CD_LINHA",
        ],
        "capture_flow": "auxiliar",
    },
    "endereco": {
        "query": """
            SELECT
                *
            FROM
                ENDERECO
            WHERE
                DT_INCLUSAO BETWEEN '{start}'
                AND '{end}'
                OR
                DT_INATIVACAO BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "principal_db",
        "primary_keys": [
            "NR_SEQ_ENDERECO",
        ],
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "capture_flow": "auxiliar",
    },
    ESTUDANTE_TABLE_ID: {
        "query": """
            SELECT
                *
            FROM
                estudante
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "gratuidade_db",
        "primary_keys": [],
        "capture_flow": "auxiliar",
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "first_timestamp": datetime(2025, 9, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
    "escola": {
        "query": """
            SELECT
                *
            FROM
                escola
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "gratuidade_db",
        "primary_keys": ["codigo_escola"],
        "capture_flow": "auxiliar",
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "first_timestamp": datetime(2025, 9, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
    LAUDO_PCD_TABLE_ID: {
        "query": """
            SELECT
                *
            FROM
                laudo_pcd
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "gratuidade_db",
        "primary_keys": ["id"],
        "capture_flow": "auxiliar",
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "first_timestamp": datetime(2025, 9, 16, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
    "ordem_ressarcimento": {
        "query": """
            SELECT
                *
            FROM
                ordem_ressarcimento
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id"],
        "capture_flow": "ordem_pagamento",
        "pretreat_funcs": [raise_if_column_isna(column_name="id_ordem_pagamento")],
    },
    "ordem_pagamento": {
        "query": """
            SELECT
                *
            FROM
                ordem_pagamento
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id"],
        "capture_flow": "ordem_pagamento",
    },
    "ordem_pagamento_consorcio_operadora": {
        "query": """
            SELECT
                *
            FROM
                ordem_pagamento_consorcio_operadora
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id"],
        "capture_flow": "ordem_pagamento",
    },
    "ordem_pagamento_consorcio": {
        "query": """
            SELECT
                *
            FROM
                ordem_pagamento_consorcio
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id"],
        "capture_flow": "ordem_pagamento",
    },
    "ordem_rateio": {
        "query": """
            SELECT
                *
            FROM
                ordem_rateio
            WHERE
                data_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id"],
        "capture_flow": "ordem_pagamento",
        "pretreat_funcs": [raise_if_column_isna(column_name="id_ordem_pagamento")],
    },
    "linha_sem_ressarcimento": {
        "query": """
            SELECT
                *
            FROM
                linha_sem_ressarcimento
            WHERE
                dt_inclusao BETWEEN '{start}'
                AND '{end}'
        """,
        "database": "ressarcimento_db",
        "primary_keys": ["id_linha"],
        "capture_flow": "ordem_pagamento",
    },
}

TRANSACAO_SOURCE = SourceTable(
    source_name=JAE_SOURCE_NAME,
    table_id=TRANSACAO_TABLE_ID,
    first_timestamp=datetime(2025, 3, 21, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    primary_keys=["id"],
)
