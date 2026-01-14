# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.common import constants as smtr_constants

JAE_SOURCE_NAME = "jae"

JAE_DATABASE_SETTINGS = {
    "principal_db": {
        "engine": "mysql",
        "host": "10.5.113.205",
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
        "host": "10.5.12.106",
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
JAE_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-jae-private", "dev": "rj-smtr-dev-private"}
ALERT_WEBHOOK = "alertas_bilhetagem"

CLIENTE_TABLE_ID = "cliente"
GRATUIDADE_TABLE_ID = "gratuidade"
ESTUDANTE_TABLE_ID = "estudante"
LAUDO_PCD_TABLE_ID = "laudo_pcd"

JAE_TABLE_CAPTURE_PARAMS = {
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
    "device_operadora": {
        "query": """
            SELECT
                dop.*,
                d.nr_serial,
                d.id_tipo_device
            FROM
                device_operadora dop
            JOIN
                device d
            ON
                dop.id_device = d.id
            /*WHERE
                dop.data_inclusao BETWEEN '{start}'
                AND '{end}'
                OR
                dop.data_desassociacao BETWEEN '{start}'
                AND '{end}'*/
        """,
        "database": "device_db",
        "primary_keys": [
            "id",
        ],
        "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        "capture_flow": "auxiliar",
        "first_timestamp": datetime(2026, 1, 13, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    },
}
