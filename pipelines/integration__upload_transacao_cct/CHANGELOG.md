# Changelog - integration__upload_transacao_cct

## [1.1.1] - 2026-06-23

### Alterado

- Passa o ambiente resolvido para `setup_dbt_queries`, mantendo o checkout de `queries/` consistente com a regra de deploy de dev/prod. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

## [1.1.0] - 2026-06-10

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.0.1] - 2026-06-09

### Adicionado

- Adiciona filtro de data na extração em caso de full refresh (https://github.com/RJ-SMTR/pipelines_v3/pull/232)

## [1.0.0] - 2026-04-13

### Adicionado

- Cria flow `integration__upload_transacao_cct` (https://github.com/RJ-SMTR/pipelines_v3/pull/112)
