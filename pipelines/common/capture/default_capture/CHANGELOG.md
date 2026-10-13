# Changelog - default_capture

## [1.0.5] - 2026-10-08

### Adicionado

- Persiste o resultado de cada extração (`timestamp_captura`, `sucesso` e `erro`) na tabela externa `<table_id>_logs`, com a mesma partição da tabela de origem. (https://github.com/RJ-SMTR/pipelines_v3/pull/751)
- Adiciona o método `get_logs_table` em `SourceTable` (https://github.com/RJ-SMTR/pipelines_v3/pull/751)

### Alterado

- `get_api_data` passa a repetir a requisição em falhas de conexão, timeout e transferência incompleta, e a rejeitar respostas JSON que não sejam objeto ou lista (https://github.com/RJ-SMTR/pipelines_v3/pull/751)
- `get_raw_api_list` passa a exigir que cada resposta seja uma lista e `get_raw_api` passa a aceitar `timeout` (https://github.com/RJ-SMTR/pipelines_v3/pull/751)
- `SourceTable.create` passa a usar `exists_ok=True`, evitando falha na criação concorrente da tabela externa (https://github.com/RJ-SMTR/pipelines_v3/pull/751)

## [1.0.4] - 2026-09-10

### Alterado

- Reutiliza o retry de `get_api_data` quando uma resposta esperada como JSON é inválida (https://github.com/RJ-SMTR/pipelines_v3/pull/650).

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
