# Changelog

## [1.1.2] - 2026-06-23

### Alterado

- Passa o ambiente resolvido para `setup_dbt_queries`, mantendo o checkout de `queries/` consistente com a regra de deploy de dev/prod. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

## [1.1.1] - 2026-06-12

### Adicionado

- Adiciona type hints nos parâmetros do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/247)

## [1.1.0] - 2026-05-27

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.0.0] - 2026-03-10

### Adicionado

- Cria flow `control__source_freshness` (https://github.com/RJ-SMTR/pipelines_v3/pull/76)
