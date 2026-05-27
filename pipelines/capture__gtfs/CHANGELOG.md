# Changelog - capture__gtfs

## [1.1.0] - 2026-05-27

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.0.1] - 2026-05-04

### Alterado

- Altera o flow `capture__gtfs` para utilizar o parâmetro `concurrency_limit` (https://github.com/RJ-SMTR/pipelines_v3/pull/146)
- Altera o flow `capture__gtfs` para utilizar o parâmetro `collision_strategy` (https://github.com/RJ-SMTR/pipelines_v3/pull/147)

## [1.0.0] - 2026-04-29

### Adicionado

- Migra flow `capture__gtfs` do Prefect 1.4 para Prefect 3 (https://github.com/RJ-SMTR/pipelines_v3/pull/127)
