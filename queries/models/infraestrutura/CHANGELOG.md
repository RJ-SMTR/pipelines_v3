# Changelog - infraestrutura

## [1.0.2] - 2026-07-20

### Alterado

- Exclui jobs com `statementType = SCRIPT` do modelo `log_bigquery` para evitar double count de bytes faturados já contabilizados nos jobs filhos (https://github.com/RJ-SMTR/pipelines_v3/pull/399)

## [1.0.1] - 2026-04-20

### Alterado

- Unifica fontes de logs do BigQuery no modelo `log_bigquery` para utilizar apenas o source `infraestrutura_staging.cloudaudit_googleapis_com_data_access`, que contempla todos os projetos (`rj-smtr`, `rj-smtr-dev`, `rj-smtr-staging`, `rj-smtr-private` e `rj-smtr-sandbox`) (https://github.com/RJ-SMTR/pipelines_v3/pull/126)

## [1.0.0] - 2025-06-26

### Adicionado

- Cria flow de materialização do dataset `infraestrutura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/549)
