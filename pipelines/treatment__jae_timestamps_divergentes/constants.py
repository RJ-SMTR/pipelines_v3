# -*- coding: utf-8 -*-
"""
Valores constantes para o flow treatment__jae_timestamps_divergentes
"""

from pipelines.capture__jae_auxiliar.flow import capture__jae_auxiliar
from pipelines.capture__jae_gps_validador.flow import capture__jae_gps_validador
from pipelines.capture__jae_lancamento.flow import capture__jae_lancamento
from pipelines.capture__jae_transacao.flow import capture__jae_transacao
from pipelines.capture__jae_transacao_erro.flow import capture__jae_transacao_erro
from pipelines.capture__jae_transacao_riocard.flow import capture__jae_transacao_riocard
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.treatment__cadastro import constants as cadastro_constants
from pipelines.treatment__cadastro.flow import treatment__cadastro
from pipelines.treatment__extrato_cliente_cartao import (
    constants as extrato_cliente_cartao_constants,
)
from pipelines.treatment__extrato_cliente_cartao.flow import treatment__extrato_cliente_cartao
from pipelines.treatment__gps_validador import constants as gps_validador_constants
from pipelines.treatment__gps_validador.flow import treatment__gps_validador
from pipelines.treatment__transacao import constants as transacao_constants
from pipelines.treatment__transacao.flow import treatment__transacao
from pipelines.treatment__transacao_erro import constants as transacao_erro_constants
from pipelines.treatment__transacao_erro.flow import treatment__transacao_erro

CAPTURE_GAP_TABLES = {
    jae_constants.TRANSACAO_TABLE_ID: {"flow": capture__jae_transacao},
    jae_constants.TRANSACAO_RIOCARD_TABLE_ID: {"flow": capture__jae_transacao_riocard},
    jae_constants.TRANSACAO_ERRO_TABLE_ID: {"flow": capture__jae_transacao_erro},
    jae_constants.GPS_VALIDADOR_TABLE_ID: {"flow": capture__jae_gps_validador},
    jae_constants.LANCAMENTO_TABLE_ID: {"flow": capture__jae_lancamento},
    jae_constants.CLIENTE_TABLE_ID: {"flow": capture__jae_auxiliar},
    jae_constants.GRATUIDADE_TABLE_ID: {"flow": capture__jae_auxiliar},
    jae_constants.ESTUDANTE_TABLE_ID: {"flow": capture__jae_auxiliar},
    jae_constants.LAUDO_PCD_TABLE_ID: {"flow": capture__jae_auxiliar},
}


CAPTURE_GAP_SELECTORS = [
    {
        "flow": treatment__cadastro,
        "capture_tables": [
            jae_constants.CLIENTE_TABLE_ID,
            jae_constants.GRATUIDADE_TABLE_ID,
            jae_constants.ESTUDANTE_TABLE_ID,
            jae_constants.LAUDO_PCD_TABLE_ID,
        ],
        "selector": cadastro_constants.CADASTRO_SELECTOR,
    },
    {
        "flow": treatment__transacao,
        "capture_tables": [
            jae_constants.TRANSACAO_TABLE_ID,
            jae_constants.TRANSACAO_RIOCARD_TABLE_ID,
        ],
        "selector": transacao_constants.TRANSACAO_SELECTOR,
    },
    {
        "flow": treatment__transacao_erro,
        "capture_tables": [jae_constants.TRANSACAO_ERRO_TABLE_ID],
        "selector": transacao_erro_constants.TRANSACAO_ERRO_SELECTOR,
    },
    {
        "flow": treatment__gps_validador,
        "capture_tables": [jae_constants.GPS_VALIDADOR_TABLE_ID],
        "selector": gps_validador_constants.GPS_VALIDADOR_SELECTOR,
    },
    {
        "flow": treatment__extrato_cliente_cartao,
        "capture_tables": [jae_constants.LANCAMENTO_TABLE_ID],
        "selector": extrato_cliente_cartao_constants.EXTRATO_CLIENTE_CARTAO_SELECTOR,
    },
]

SQL_TREATMENTS = [
    {
        "capture_tables": [
            jae_constants.CLIENTE_TABLE_ID,
            jae_constants.GRATUIDADE_TABLE_ID,
            jae_constants.ESTUDANTE_TABLE_ID,
            jae_constants.LAUDO_PCD_TABLE_ID,
        ],
        "sql": """
            UPDATE {project}.bilhetagem.transacao t
            SET
            tipo_usuario = nv.tipo_usuario,
            subtipo_usuario = nv.subtipo_usuario,
            subtipo_usuario_protegido = nv.subtipo_usuario_protegido,
            documento_cliente = nv.documento_cliente,
            tipo_documento_cliente = nv.tipo_documento_cliente,
            datetime_ultima_atualizacao = IF(
                t.subtipo_usuario IS DISTINCT FROM nv.subtipo_usuario
                OR t.documento_cliente IS DISTINCT FROM nv.documento_cliente
                OR t.tipo_documento_cliente IS DISTINCT FROM nv.tipo_documento_cliente
                OR t.subtipo_usuario IS DISTINCT FROM nv.subtipo_usuario
                OR t.subtipo_usuario_protegido
                    IS DISTINCT FROM nv.subtipo_usuario_protegido,
                CURRENT_DATETIME("America/Sao_Paulo"),
                t.datetime_ultima_atualizacao)
            FROM
            (
                SELECT
                t.id_transacao,  -- ou outra PK única
                c.documento as documento_cliente,
                c.tipo_documento as tipo_documento_cliente,
                CASE
                    WHEN
                    t.tipo_transacao_jae NOT LIKE "%Gratuidade%"
                    AND (
                        t.produto_jae != "Conta Jaé Gratuidade" OR t.produto_jae IS NULL)
                    THEN "Pagante"
                    WHEN
                    t.tipo_transacao_jae IN (
                        "Gratuidade operador pcd", "Gratuidade acompanhante")
                    OR g.tipo_gratuidade = "PCD"
                    THEN "Saúde"
                    WHEN t.tipo_transacao_jae = "Gratuidade operador estudante"
                    THEN "Estudante"
                    WHEN
                    g.tipo_gratuidade = "Sênior"
                    OR t.tipo_transacao_jae = "Gratuidade operador sênior"
                    THEN "Idoso"
                    WHEN tipo_transacao_jae LIKE "Gratuidade operador%"
                    THEN "Operadora"
                    ELSE g.tipo_gratuidade
                    END AS tipo_usuario,
                CASE
                    WHEN
                    t.tipo_transacao_jae = "Gratuidade acompanhante"
                    OR (
                        t.tipo_transacao_jae NOT LIKE "%Gratuidade%"
                        AND (
                        t.produto_jae != "Conta Jaé Gratuidade"
                        OR t.produto_jae IS NULL))
                    THEN NULL
                    WHEN
                    g.tipo_gratuidade = "Estudante"
                    AND g.rede_ensino LIKE "Universidade%"
                    THEN "Ensino Superior"
                    WHEN g.tipo_gratuidade = "Estudante" AND g.rede_ensino IS NOT NULL
                    THEN concat("Ensino Básico ", split(g.rede_ensino, " - ")[0])
                    END AS subtipo_usuario,
                CASE
                    WHEN t.tipo_transacao_jae = "Gratuidade acompanhante"
                    THEN "Acompanhante"
                    WHEN
                    t.tipo_transacao_jae != "Gratuidade"
                    AND t.produto_jae != "Conta Jaé Gratuidade"
                    THEN NULL
                    WHEN
                    g.tipo_gratuidade = "Estudante"
                    AND g.rede_ensino LIKE "Universidade%"
                    THEN concat("Ensino Superior ", g.rede_ensino)
                    WHEN g.tipo_gratuidade = "Estudante" AND g.rede_ensino IS NOT NULL
                    THEN concat("Ensino Básico ", split(g.rede_ensino, " - ")[0])
                    WHEN g.tipo_gratuidade = "PCD" AND g.deficiencia_permanente
                    THEN "PCD"
                    WHEN g.tipo_gratuidade = "PCD" AND NOT g.deficiencia_permanente
                    THEN "DC"
                    END AS subtipo_usuario_protegido,
                FROM rj-smtr.bilhetagem.transacao t
                JOIN rj-smtr.cadastro_interno.cliente_jae c ON t.id_cliente = c.id_cliente
                JOIN rj-smtr.bilhetagem_staging.aux_gratuidade_info g
                ON
                    t.id_cliente = CAST(g.id_cliente AS STRING)
                    AND t.datetime_transacao >= g.datetime_inicio_validade
                    AND (
                    t.datetime_transacao < g.datetime_fim_validade
                    OR g.datetime_fim_validade IS NULL)
                WHERE
                    t.data >= date_sub(date("{datetime_start}"), INTERVAL 1 DAY)
            ) nv
            WHERE
                t.id_transacao = nv.id_transacao
                AND t.data >= date_sub(date("{datetime_start}"), INTERVAL 1 DAY)
        """,
    },
    {
        "capture_tables": [jae_constants.CLIENTE_TABLE_ID],
        "sql": """
            UPDATE {project}.bilhetagem_interno.extrato_cliente_cartao e
            SET nome_cliente = nv.nome_cliente,
            documento_cliente = nv.documento_cliente,
            tipo_documento_cliente = nv.tipo_documento_cliente,
            datetime_ultima_atualizacao = IF(
                e.nome_cliente IS DISTINCT FROM nv.nome_cliente
                OR e.documento_cliente IS DISTINCT FROM nv.documento_cliente
                OR e.tipo_documento_cliente IS DISTINCT FROM nv.tipo_documento_cliente,
                CURRENT_DATETIME("America/Sao_Paulo"),
                e.datetime_ultima_atualizacao)
            FROM
            (
                SELECT
                e.id_unico_lancamento,
                e.id_conta,
                c.nome as nome_cliente,
                c.documento as documento_cliente,
                c.tipo_documento as tipo_documento_cliente,
                FROM rj-smtr.bilhetagem_interno.extrato_cliente_cartao e
                JOIN rj-smtr.cadastro_interno.cliente_jae c ON e.id_cliente = c.id_cliente
                WHERE
                e.data >= date_sub(date("{datetime_start}"), INTERVAL 1 DAY)
            ) nv
            WHERE
            e.id_unico_lancamento = nv.id_unico_lancamento
            and e.id_conta = nv.id_conta
            AND e.data >= date_sub(date("{datetime_start}"), INTERVAL 1 DAY)
        """,
    },
]
