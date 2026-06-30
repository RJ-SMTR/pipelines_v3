# Changelog - default_treatment

## [1.4.0] - 2026-06-23

### Adicionado

- Adiciona criação seletiva de relações dbt ausentes com `--empty` antes da materialização normal em ambiente dev. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)
- Adiciona resolução do SHA do deployment para clonar `queries/` no mesmo commit da imagem em deploys de dev. (https://github.com/RJ-SMTR/pipelines_v3/pull/292)

## [1.3.0] - 2026-06-02

### Adicionado

- Adiciona tasks `setup_dbt_queries` e `install_dbt_packages` ao flow genérico de materialização para download da pasta `queries/` e instalação de pacotes dbt em runtime (https://github.com/RJ-SMTR/pipelines_v3/pull/214)

## [1.2.2] - 2026-06-02

### Adicionado

- Adiciona parâmetro `test_only` (https://github.com/RJ-SMTR/pipelines_v3/pull/223)

### Corrigido

- Altera should_run da classe `DBTSelectorMaterializationContext` para `self.datetime_start >= selector.final_datetime` (https://github.com/RJ-SMTR/pipelines_v3/pull/223)

## [1.2.1] - 2026-05-11

### Alterado

- Suporta testes singulares na notificação do Discord em `dbt_test_notify_discord` (https://github.com/RJ-SMTR/pipelines_v3/pull/170)

## [1.2.0] - 2026-03-30

### Adicionado

- Adiciona parâmetro `fallback_run` (https://github.com/RJ-SMTR/pipelines_v3/pull/102)

## [1.1.0] - 2026-03-26

### Adicionado

- Cria funções `run_dbt_tests` e `dbt_test_notify_discord` (https://github.com/RJ-SMTR/pipelines_v3/pull/97)
- Adiciona atributos `test_start_datetime` e `test_alias` na classe `DBTTest` (https://github.com/RJ-SMTR/pipelines_v3/pull/97)

### Alterado

- Renomeia task `run_dbt_tests` para `run_dbt_selector_tests` e `dbt_test_notify_discord` para `task_dbt_selector_test_notify_discord` (https://github.com/RJ-SMTR/pipelines_v3/pull/97)
- Utiliza novas funções nas tasks `run_dbt_selector_tests` e `task_dbt_selector_test_notify_discord` (https://github.com/RJ-SMTR/pipelines_v3/pull/97)

## [1.0.2] - 2026-02-24

### Alterado

- Remove map da task `save_materialization_datetime_redis` (https://github.com/RJ-SMTR/pipelines_v3/pull/52)
- Altera função para retornar secrets para `get_env_secrets` (https://github.com/RJ-SMTR/pipelines_v3/pull/52)

## [1.0.1] - 2026-02-03

### Alterado

- Ajusta dependências das tasks do flow `default_treatment` (https://github.com/RJ-SMTR/pipelines_v3/pull/40)

## [1.0.0] - 2026-01-05

### Adicionado

- Cria flow genérico de materialização (https://github.com/RJ-SMTR/pipelines_v3/pull/13)
