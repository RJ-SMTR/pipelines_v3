# Changelog

## [1.1.1] - 2026-06-12

### Adicionado

- Adiciona type hints nos parâmetros do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/247)

## [1.1.0] - 2026-05-27

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.0.1] - 2026-05-21

### Adicionado

- Substitui `datetime.now()` por `get_scheduled_timestamp()` e amplia janela de teste de 1h para 2h para evitar race condition com captura na virada do dia (https://github.com/RJ-SMTR/pipelines_v3/pull/202)

## [1.0.0] - 2026-05-13

### Adicionado

- Cria flow `control__model_freshness` (https://github.com/RJ-SMTR/pipelines_v3/pull/181)
