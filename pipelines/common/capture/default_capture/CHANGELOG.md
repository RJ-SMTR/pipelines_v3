# Changelog - default_capture

## [1.0.3] - 2026-06-23

### Adicionado

- Adiciona suporte ao parâmetro opcional `should_capture_task` em `create_capture_flows_default_tasks`, permitindo interromper a captura antes da extração quando a fonte não tiver dados novos. (https://github.com/RJ-SMTR/pipelines_v3/pull/306)

## [1.0.2] - 2026-06-12

### Alterado

- Adiciona parâmetro `timeout` configurável em `get_api_data` e `get_raw_api_list` (https://github.com/RJ-SMTR/pipelines_v3/pull/246)

## [1.0.1] - 2026-02-24

### Alterado

- Torna parâmetro source_table_ids opcional (https://github.com/RJ-SMTR/pipelines_v3/pull/52)

## [1.0.0] - 2025-11-24

### Adicionado

- Cria flow genérico de captura (https://github.com/RJ-SMTR/pipelines_v3/pull/4)
