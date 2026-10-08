# Changelog - capture\_\_rioonibus_viagem_informada

## [1.0.3] - 2026-10-08

### Alterado

- Migra a captura para a API v1.1 da Rio Ônibus, com novo endpoint e secret `rioonibus_api_v2` (https://github.com/RJ-SMTR/pipelines_v3/pull/751)
- Altera a extração para uma única requisição com o dia anterior em `America/Sao_Paulo`, enviado em UTC e com fim exclusivo, conforme a especificação da API (https://github.com/RJ-SMTR/pipelines_v3/pull/751)
- Aumenta o timeout da requisição para 600s e do flow para 2h (https://github.com/RJ-SMTR/pipelines_v3/pull/751)

## [1.0.2] - 2026-06-12

### Alterado

- Aumenta timeout da API para 300s para suportar dias com alto volume de viagens (https://github.com/RJ-SMTR/pipelines_v3/pull/246)

### Adicionado

- Adiciona type hints nos parâmetros do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/247)

## [1.0.1] - 2026-06-10

### Adicionado

- Atualiza endpoint da API da Rio Ônibus (https://github.com/RJ-SMTR/pipelines_v3/pull/237)

## [1.0.0] - 2026-03-02

### Adicionado

- Migração do flow `capture__viagem_informada` do Prefect 1.4 para Prefect 3.0 (https://github.com/RJ-SMTR/pipelines_v3/pull/61)
