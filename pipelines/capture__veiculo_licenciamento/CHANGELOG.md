# Changelog

## [1.0.4] - 2026-11-26

### Corrigido

- No pré-tratamento, a linha com `id_veiculo` igual a `NULL` é descartada, como na leitura anterior com pandas; a captura passou a ler o raw com Polars, que mantém o texto `NULL` (https://github.com/RJ-SMTR/pipelines_v3/pull/897)

## [1.0.3] - 2026-09-11

### Alterado

- Alterado Alterado recapture_days de 1 para 2 (https://github.com/RJ-SMTR/pipelines_v3/pull/667)

## [1.0.2] - 2026-09-09

### Alterado

- Alterado o schedule do flow de 5h para 6h (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

### Adicionado

- Adiciona schedule de recaptura às 6h15 (`recapture_days: 1`) para o dia anterior (https://github.com/RJ-SMTR/pipelines_v3/pull/658)

## [1.0.1] - 2026-06-12

### Adicionado

- Adiciona type hints nos parâmetros do flow (https://github.com/RJ-SMTR/pipelines_v3/pull/247)

## [1.0.0] - 2026-04-16

### Adicionado

- Cria flow capture__veiculo_licenciamento (https://github.com/RJ-SMTR/pipelines_v3/pull/103)
