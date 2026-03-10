# -*- coding: utf-8 -*-
"""
Constantes para backup incremental de dados BillingPay da Jaé
"""

from zoneinfo import ZoneInfo

# ============================================================================
# Timezone
# ============================================================================
TIMEZONE = ZoneInfo("America/Sao_Paulo")

# ============================================================================
# Table validation
# ============================================================================
MAX_UNFILTERED_TABLE_ROWS = 5000

# ============================================================================
# Secrets and Access
# ============================================================================
JAE_SECRET_PATH = "smtr_jae_access_data"
ALERT_WEBHOOK = "alertas_bilhetagem"

# ============================================================================
# Database Settings
# ============================================================================
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
    "vendas_db": {
        "engine": "postgresql",
        "host": "10.5.114.15",
    },
}

# ============================================================================
# Backup Configuration
# ============================================================================
BACKUP_BILLING_PAY_FOLDER = "backup_jae_billingpay"
BACKUP_BILLING_LAST_VALUE_REDIS_KEY = "last_backup_value"

# Databases that execute every 6 hours (critical)
MULTIPLE_EXECUTION_DBS = ["processador_transacao_db", "financeiro_db", "midia_db"]

BACKUP_JAE_BILLING_PAY = {
    "principal_db": {
        "exclude": [
            "LINHA",
            "OPERADORA_TRANSPORTE",
            "CLIENTE",
            "PESSOA_FISICA",
            "CONSORCIO",
            "LINHA_CONSORCIO",
            "LINHA_CONSORCIO_OPERADORA_TRANSPORTE",
            "ENDERECO",
            "check_cadastro_pcd_validado",
            "importa_pcd_pf",
            "gratuidade_import_pcd",
            "recarga_duplicada",
            "SEQUENCIA_SERVICO",
            "CLIENTE_FRAUDE_05092024",
            "stops_with_routes",
            "cliente_com_data_nascimento",
            "vt_verificar_cpf_setempedido_cartao",
            "Linhas_empresa_csv",
            "acerto_pedido_2",
            "routes",
            "fare_rules",
            "estudante_12032025",
            "temp_estudante_cpfduplicado_14032025",
            "estudante_11042025",
            "estudante_01042025",
            "temp_estudante_cpfduplicado_11042025",
            "estudante_30042025",
            "estudante_24062025",
            "estudante_20062025",
            "producao_20250617081705_02_VT",
            "temp_estudante_15082025",
            "estudante_11072025",
            "temp_cliente_02082025",
            "temp_requisicao_pedido_ticketeira",
            "temp_estudante_27082025",
            "temp_pedido_VT_12092025",
            "temp_estudante_02102025",
            "temp_VT_sem_retorno_16102025",
            "temp_cliente_fraudes_14102025",
            "temp_vt_sem_retorno_13102025",
            "temp_bloqueio_cliente_14102025",
            "temp_estudante_31102025",
            "temp_logistica_limbo_25102025",
            "temp_cartoes_transferidos_valorados_18112025",
            "temp_vt_limbo_24102025",
            "temp_estudante_05122025",
            "temp_midias_transferidas_04122025",
        ],
        "filter": {
            "ITEM_PEDIDO": ["DT_INCLUSAO"],
            "CLIENTE_CONTA_ACESSO": ["DT_INCLUSAO"],
            "CLIENTE_PERFIL": ["DT_CADASTRO"],
            "PEDIDO": [
                "DT_CONCLUSAO_PEDIDO",
                "DT_CANCELAMENTO",
                "DT_PAGAMENTO",
                "DT_INCLUSAO",
            ],
            "SERVICO_MOTORISTA": [
                "DT_ABERTURA",
                "DT_FECHAMENTO",
            ],
            "CONTROLE_PAGAMENTO_PEDIDO": [
                "DT_PAGAMENTO",
                "DT_BAIXA",
                "DT_CREDITO",
                "DT_INCLUSAO",
            ],
            "RESUMO_FECHAMENTO_SERVICO": [
                "DT_ABERTURA",
                "DT_FECHAMENTO",
            ],
            "CLIENTE_IMAGEM": [
                "DT_INCLUSAO",
                "DT_ALTERACAO",
            ],
            "IMPORTA_DET_LOTE_VT": ["DT_INCLUSAO"],
            "ITEM_PEDIDO_ENDERECO": ["DT_INCLUSAO"],
            "CLIENTE_FAVORECIDO": [
                "DT_CANCELAMENTO",
                "DT_INCLUSAO",
            ],
            "IMPORTA_DET_LOTE_VT_ERRO": ["DT_INCLUSAO"],
            "ERRO_IMPORTACAO_COLABORADOR_DETALHE": ["DT_CRIACAO"],
            "ERRO_IMPORTACAO_COLABORADOR": ["CD_ERRO"],
            "IMPORTA_LOTE_VT": ["DT_INCLUSAO"],
            "PESSOA_JURIDICA": ["CD_CLIENTE"],
            "ERRO_IMPORTACAO_PEDIDO_DETALHE": ["DT_CRIACAO"],
            "ERRO_IMPORTACAO_PEDIDO": ["CD_ERRO"],
            "MOTORISTA_OPERADORA": [
                "DT_ASSOCIACAO",
                "DT_FIM_ASSOCIACAO",
            ],
            "MOTORISTA": ["CD_MOTORISTA"],
            "IMPORTACAO_ARQUIVO": ["DT_INCLUSAO"],
            "GRUPO_LINHA": [
                "DT_FIM_VALIDADE",
                "DT_INCLUSAO",
            ],
            "CLIENTE_DEPENDENTE": [
                "DT_INCLUSAO",
                "DT_CANCELAMENTO",
            ],
            "pcd_mae": ["count(*)"],
        },
        "custom_select": {
            "CLIENTE_IMAGEM": """
                select
                    *
                from CLIENTE_IMAGEM
                where ID_CLIENTE_IMAGEM IN (
                    select distinct
                        ID_CLIENTE_IMAGEM
                    from CLIENTE_IMAGEM
                    where {filter}
                )
            """,
        },
        "page_size": {"CLIENTE_IMAGEM": 500},
    },
    "tarifa_db": {
        "exclude": ["linha_tarifa"],
        "filter": {
            "matriz_integracao": ["dt_inclusao"],
        },
    },
    "transacao_db": {
        "exclude": [
            "transacao",
            "transacao_riocard",
            "embossadora_producao_20240809",
            "transacao_faltante_23082023",
            "temp_estudante_cpfduplicado_13032025",
            "temp_estudante_cpfduplicado_14032025",
            "temp_estudante_cpfduplicado_17032025",
        ],
        "filter": {
            "confirmacao_envio_pms": ["data_confirmacao"],
            "spatial_ref_sys": ["count(*)"],
            "us_rules": ["count(*)"],
            "us_lex": ["count(*)"],
            "us_gaz": ["count(*)"],
            "midia_jall": ["count(*)"],
        },
    },
    "tracking_db": {
        "exclude": [
            "tracking_detalhe",
        ],
        "filter": {
            "tracking_sumarizado": ["ultima_data_tracking"],
            "spatial_ref_sys": ["srid"],
            "mq_connections": ["count(*)"],
        },
    },
    "ressarcimento_db": {
        "exclude": [
            "integracao_transacao",
            "ordem_ressarcimento",
            "ordem_pagamento",
            "ordem_pagamento_consorcio_operadora",
            "ordem_pagamento_consorcio",
            "ordem_rateio",
            "linha_sem_ressarcimento",
            "percentual_rateio_integracao",
        ],
        "filter": {
            "item_ordem_transferencia_custodia": ["data_inclusao"],
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "item_ordem_transferencia_custodia_old": ["data_inclusao"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        },
    },
    "gratuidade_db": {
        "exclude": [
            "gratuidade",
            "estudante_import_old",
            "gratuidade_import_pcd_old",
            "estudante_seeduc_25032025",
            "pcd_excluir",
        ],
        "filter": {
            "estudante": ["data_atualizacao"],
            "laudo_pcd": ["data_atualizacao"],
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        },
    },
    "fiscalizacao_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "atm_gateway_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "device_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "erp_integracao_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "financeiro_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "midia_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "processador_transacao_db": {
        "exclude": [
            "transacao_lote_processamento",
        ],
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        },
    },
    "atendimento_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "gateway_pagamento_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
    "vendas_db": {
        "filter": {
            "batch_step_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_step_execution_context": ["step_execution_id"],
            "batch_job_execution_params": ["job_execution_id"],
            "batch_job_instance": ["job_instance_id"],
            "batch_job_execution": [
                "create_time",
                "last_updated",
            ],
            "batch_job_execution_context": ["job_execution_id"],
        }
    },
}
