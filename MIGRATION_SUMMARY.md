# Migração: monitoramento_temperatura para Prefect 3.0

Data: 2026-04-16

## Overview

Migração completa do pipeline `monitoramento_temperatura - materializacao` de Prefect 1.4 para Prefect 3.0, incluindo:
- Pipeline flow com nova estrutura
- Modelos dbt relacionados
- Testes dbt customizados
- Snapshots

## Estrutura de Pipeline

### Código Python
```
pipelines/treatment__monitoramento_temperatura/
├── flow.py              # Flow principal
├── constants.py         # Constantes e seletores dbt
├── prefect.yaml         # Configuração de deployments
├── pyproject.toml       # Metadados do pacote
├── Dockerfile           # Imagem de deploy
├── CHANGELOG.md         # Histórico
└── .dockerignore        # Exclusões Docker
```

**Schedule**: Diariamente às 6h00 (São Paulo)  
**Dependência**: treatment__monitoramento_veiculo  
**Tipos de variáveis adicionais**: `tipo_materializacao="monitoramento"`

### Modelos dbt

#### Modelos Principais
- `temperatura.sql` - Dados de temperatura de INMET e Alerta Rio (incremental)
- `veiculo_regularidade_temperatura_dia.sql` - Indicadores de regularidade por veículo (incremental)

#### Modelos Staging
- `temperatura_inmet.sql` - Temperatura do INMET com fallback para staging (incremental)
- `temperatura_alertario.sql` - Temperatura do sistema Alerta Rio (incremental)
- `staging_temperatura_inmet.sql` - Fallback de temperatura INMET (view)
- `aux_veiculo_falha_ar_condicionado.sql` - Auxiliar de falhas de ar condicionado (incremental)

#### Modelos de Viagem (Subsidio)
- `aux_viagem_temperatura.sql` - Auxiliar com indicadores de temperatura por viagem (incremental)

#### Snapshots
- `snapshot_temperatura` - Snapshot da tabela temperatura
- `snapshot_temperatura_alertario` - Snapshot dos dados do Alerta Rio
- `snapshot_temperatura_inmet` - Snapshot dos dados do INMET
- `snapshot_viagem_regularidade_temperatura` - Snapshot da regularidade por viagem

### Testes Customizados
- `test_completude_temperatura` - Valida presença de dados em todas as 24 horas
- `test_check_regularidade_temperatura` - Valida indicadores de regularidade
- `test_consistencia_indicadores_temperatura` - Valida consistência entre indicadores

## Validações Aplicadas

✅ Sintaxe Python compilada sem erros  
✅ Ruff lint passou  
✅ Todos os arquivos SQL formatados com sqlfmt  
✅ Estrutura dbt validada  
✅ Arquivos de configuração completos  

## Próximos Passos

1. Verificar dependências de dados_sources se houver
2. Testar o flow em ambiente staging
3. Validar schedules e triggers de dependência
4. Commits e deploy

## Notas de Migração

- O flow usa `create_materialization_flows_default_tasks()` simplificando a orquestração
- Snapshots estão inclusos para auditoria de histórico
- Variáveis adicionais são configuradas automaticamente no flow
- As dependências dbt são gerenciadas via manifest automático
