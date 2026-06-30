# Changelog - control__jae_verificacao_captura

## [1.0.3] - 2026-05-21

### Alterado

- Altera função `get_jae_timestamp_captura_count` para usar a nova função `get_jae_database_settings` (https://github.com/RJ-SMTR/pipelines_v3/pull/201)

## [1.0.2] - 2026-05-20

### Adicionado

- Adiciona parâmetro `save_results` em `get_capture_gaps` para tornar opcional a persistência do resultado da verificação na tabela de controle (https://github.com/RJ-SMTR/pipelines_v3/pull/185)

## [1.0.1] - 2026-03-27

### Corrigido

- Altera nome da tabela temporária do resultado da verificação (https://github.com/RJ-SMTR/pipelines_v3/pull/98)

## [1.0.0] - 2026-03-17

### Adicionado

- Cria flow `control__jae_verificacao_captura` (https://github.com/RJ-SMTR/pipelines_v3/pull/90)
