# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-10

### Added

- **Migração para Prefect 3**: Conversão completa do flow `capture__jae_backup_billingpay` de Prefect 1.4 para Prefect 3.0
- **Backup incremental**: Suporte para backup incremental de múltiplas bases de dados JAE (processador_transacao_db, financeiro_db, midia_db e outras)
- **Rastreamento via Redis**: Controle incremental com valores armazenados em Redis
- **Validação de tabelas**: Detecção de tabelas sem filtro configurado (>5000 registros) com alertas Discord
- **Schedules configuráveis**: Execução em cron com intervalos diferentes por banco de dados:
  - 6 horas para bases críticas (processador_transacao_db, financeiro_db, midia_db)
  - 24 horas para demais bases
- **Paginação automática**: Processamento de grandes volumes com paginação configurável
- **Storage GCS**: Upload de backups para Google Cloud Storage com particionamento automático

### Features

- `rename_flow_run_backup_billingpay`: Renomeação dinâmica da execução do flow
- `get_jae_db_config`: Configuração automática de conexão com bases JAE
- `get_table_info`: Inventário e validação de tabelas com filtros
- `get_non_filtered_tables`: Alertas para tabelas sem filtro configurado
- `create_non_filtered_discord_message`: Mensagens de alerta formatadas
- `get_raw_backup_billingpay`: Extração de dados com incrementais (datetime/integer/count)
- `upload_backup_billingpay`: Upload com mapeamento automático de múltiplas tabelas
- `set_redis_backup_billingpay`: Sincronização de última captura no Redis

### Changed

- Refatoração de `@task` para sintaxe Prefect 3 (sem max_retries, retry_delay)
- Migração de logging de `log()` para `print()` com `@flow(log_prints=True)`
- Conversão de `Enum` para variáveis simples SCREAMING_SNAKE_CASE
- Simplificação de estruturas: `Parameter` → argumentos de função
- Remoção de `with case()` condicional → `if` simples
- Atualização de imports: `prefect`, `iplanrio.pipelines_utils.env`

### Technical Details

- **Python 3.13** | **Prefect 3.4.9** | **uv workspaces**
- Conformidade total com CLAUDE.md
- Type hints em 100% dos parâmetros e retornos
- Timezone-aware datetime com `zoneinfo.ZoneInfo`
- Sem `__init__.py` no diretório pipeline

### Database Support

Suporta backup para 17 bases JAE:
- principal_db (MySQL)
- tarifa_db, transacao_db, tracking_db, ressarcimento_db, gratuidade_db, fiscalizacao_db
- atm_gateway_db, device_db, erp_integracao_db, financeiro_db, midia_db
- processador_transacao_db, atendimento_db, gateway_pagamento_db, vendas_db

---

**Status**: Migrado e testado ✓
**Compatibilidade**: Prefect 3.0+ | GCP BigQuery | SMTR infra
