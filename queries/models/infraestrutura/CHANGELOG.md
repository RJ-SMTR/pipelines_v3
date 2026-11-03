# Changelog - infraestrutura

## [1.1.0] - 2026-10-29

### Adicionado

- Adiciona `datetime_inicio_job`, `datetime_fim_job` e `id_job_pai` ao `log_bigquery`, com horários em `America/Sao_Paulo`. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)
- Adiciona `tipo_statement`, `status_job`, `codigo_erro_job`, `mensagem_erro_job`, `tabela_destino`, `tabelas_referenciadas` e `labels` ao `log_bigquery` para uso como trilha de auditoria. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)
- Adiciona teste de unicidade de `id_job`. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)
- Adiciona o modelo ephemeral `aux_logs_bigquery`, que une os logs de auditoria dos datasets `bq_logs` de `rj-smtr`, `rj-smtr-dev` e `rj-smtr-staging` até 2026-04-01 com a sink unificada `infraestrutura_staging` a partir de `data_inicio_sink_unificada_logs_bigquery`. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)
- Adiciona a variável `data_inicio_sink_unificada_logs_bigquery` (2026-04-02), pois a sink unificada só passou a receber dados em 2026-04-01 às 13:06 UTC. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)

### Alterado

- Atribui os jobs filhos ao processo e à execução dbt do pai quando não possuem identificação própria, preservando o filtro de `SCRIPT` na saída e a soma dos bytes faturados. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)
- Seleciona os eventos `jobservice.jobcompleted` e deduplica por `id_job` antes de expandir labels e calcular custos. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)
- Lê os logs de `aux_logs_bigquery`, permitindo reconstruir o histórico completo desde 2024-09-30 com `--full-refresh`. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)

### Removido

- Remove do histórico os jobs `SCRIPT` e os eventos de outros métodos que haviam permanecido na tabela por versões antigas do modelo, evitando dupla contagem de bytes faturados. (https://github.com/RJ-SMTR/pipelines_v3/pull/835)

## [1.0.3] - 2026-09-10

### Removido

- Remove os filtros de usuário não nulo e bytes faturados maiores que zero do modelo `log_bigquery`, mantendo a exclusão de jobs com `statementType = SCRIPT` (https://github.com/RJ-SMTR/pipelines_v3/pull/662)

## [1.0.2] - 2026-07-20

### Alterado

- Exclui jobs com `statementType = SCRIPT` do modelo `log_bigquery` para evitar contagem duplicada de bytes faturados já contabilizados nos jobs filhos (https://github.com/RJ-SMTR/pipelines_v3/pull/399)

## [1.0.1] - 2026-04-20

### Alterado

- Unifica fontes de logs do BigQuery no modelo `log_bigquery` para utilizar apenas o source `infraestrutura_staging.cloudaudit_googleapis_com_data_access`, que contempla todos os projetos (`rj-smtr`, `rj-smtr-dev`, `rj-smtr-staging`, `rj-smtr-private` e `rj-smtr-sandbox`) (https://github.com/RJ-SMTR/pipelines_v3/pull/126)

## [1.0.0] - 2025-06-26

### Adicionado

- Cria flow de materialização do dataset `infraestrutura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/549)
