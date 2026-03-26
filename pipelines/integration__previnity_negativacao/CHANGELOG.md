# Changelog - integration__previnity_negativacao

## [1.1.1] - 2026-03-26

### Alterado

- Renomeia task `run_dbt_tests` para `run_dbt_selector_tests` e `dbt_test_notify_discord` para `task_dbt_selector_test_notify_discord` (https://github.com/RJ-SMTR/pipelines_v3/pull/97)

## [1.1.0] - 2026-03-23

### Adicionado

- Adiciona teste `test_consistencia_autuacoes_negativadas_pagas_sem_baixa` para verificar se todas as autuações pagas foram devidamente baixadas da negativação (https://github.com/RJ-SMTR/pipelines_v3/pull/94)

## [1.0.2] - 2026-02-26

### Alterado

- Adiciona filtro de datas na query `QUERY_PF` para buscar apenas registros no intervalo de execução, melhorando a performance do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/56)

## [1.0.1] - 2026-02-24

### Alterado

- Altera função para retornar secrets para `get_env_secrets` (https://github.com/RJ-SMTR/pipelines_v3/pull/52)

## [1.0.0] - 2026-02-02

### Adicionado

- Cria flow `integration__previnity_negativacao` (https://github.com/RJ-SMTR/pipelines_v3/pull/8)
