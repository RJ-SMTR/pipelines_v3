# treatment__monitoramento_temperatura

Pipeline de materialização dos dados de monitoramento de temperatura no BigQuery.

## Descrição

Este pipeline executa a materialização incremental de modelos dbt relacionados ao monitoramento de temperatura de ônibus, incluindo:

- **temperatura**: Dados de temperatura de estações meteorológicas (INMET e Alerta Rio)
- **veiculo_regularidade_temperatura_dia**: Indicadores de regularidade do ar condicionado por veículo
- **aux_viagem_temperatura**: Auxiliar com métricas de temperatura por viagem
- **aux_veiculo_falha_ar_condicionado**: Identificação de veículos com falhas

## Schedule

**Produção**: Diariamente às 6h00 (horário de São Paulo)

```
cron: "0 6 * * *"
timezone: "America/Sao_Paulo"
```

## Dependências

### De Dados
- `treatment__monitoramento_veiculo`: Deve ser executado antes deste pipeline

### De Fontes Externas
- `clima_estacao_meteorologica.meteorologia_inmet`: Dados meteorológicos INMET
- `clima_estacao_meteorologica.meteorologia_alertario`: Dados do Sistema Alerta Rio
- `jae.bilhetagem_viagem`: Dados de bilhetagem de viagens (Validadores JAé)

## Variáveis de Ambiente

### Variáveis Automáticas (passadas pelo flow)
- `tipo_materializacao`: "monitoramento" (configurado no flow)

### Variáveis dbt Necessárias
- `date_range_start`: Data inicial do período (formatada automaticamente)
- `date_range_end`: Data final do período (formatada automaticamente)
- `DATA_SUBSIDIO_V17_INICIO`: Data de início do subsídio v17 (configurado em dbt_project.yml)
- `DATA_SUBSIDIO_V19_INICIO`: Data de início do subsídio v19
- `DATA_SUBSIDIO_V21_INICIO`: Data de início do subsídio v21
- `version`: Versão do modelo

## Execução Local

```bash
# Setup
uv sync --all-packages
source .venv/bin/activate

# Executar o flow localmente
uv run --package treatment__monitoramento_temperatura -- prefect flow-run execute

# Com parâmetros
uv run --package treatment__monitoramento_temperatura -- prefect flow-run execute \
  --env datetime_start="2024-01-01" \
  --env datetime_end="2024-01-31"
```

## Monitoramento

Testes executados após materialização (6 modelos):

- **temperatura_inmet**: not_null em data/hora, test_completude
- **temperatura_alertario**: not_null em data/hora
- **aux_viagem_temperatura**: not_null, unique combination
- **aux_veiculo_falha_ar_condicionado**: not_null, unique combination
- **veiculo_regularidade_temperatura_dia**: not_null, unique combination
- **temperatura**: not_null, test_completude (24h validation)

## Estrutura do Projeto

```
pipelines/treatment__monitoramento_temperatura/
├── flow.py                    # Flow principal do Prefect
├── constants.py               # Constantes de configuração dbt
├── prefect.yaml              # Configuração de deployments
├── pyproject.toml            # Metadados do pacote
├── Dockerfile                # Imagem de execução
├── CHANGELOG.md              # Histórico de mudanças
└── README.md                 # Este arquivo
```

## Modelos dbt

Os modelos estão em `queries/models/` e `queries/snapshots/`:

### Materializações Principais
- `models/monitoramento/temperatura.sql` - Temperature data incremental
- `models/monitoramento/veiculo_regularidade_temperatura_dia.sql` - Vehicle regularity

### Staging
- `models/monitoramento/staging/temperatura_inmet.sql`
- `models/monitoramento/staging/temperatura_alertario.sql`
- `models/monitoramento/staging/staging_temperatura_inmet.sql` (contingency)
- `models/monitoramento/staging/aux_veiculo_falha_ar_condicionado.sql`

### Cross-Pipeline
- `models/subsidio/staging/aux_viagem_temperatura.sql`

### Snapshots (Auditoria)
- `snapshots/monitoramento/snapshot_temperatura.sql`
- `snapshots/monitoramento/snapshot_temperatura_alertario.sql`
- `snapshots/monitoramento/snapshot_temperatura_inmet.sql`

## Troubleshooting

### Erro: "MONITORAMENTO_VEICULO não disponível"
Certificar que o pipeline `treatment__monitoramento_veiculo` foi executado com sucesso antes deste.

### Erro: "Variável DATA_SUBSIDIO_V17_INICIO não definida"
Verificar `queries/dbt_project.yml` para variáveis necessárias ou passar via CLI:
```bash
dbt build --vars '{"DATA_SUBSIDIO_V17_INICIO": "2017-01-01"}'
```

### Performance Lenta
Os modelos usam `incremental` com `insert_overwrite`. Verificar:
- Particionamento por `data`
- Período de execução (últimos 7-30 dias geralmente)
- Estatísticas de tabelas no BigQuery

## Referências

- [Prefect 3 Docs](https://docs.prefect.io/v3/)
- [dbt BigQuery Plugin](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)
- [CLAUDE.md](../../CLAUDE.md) - Project standards
