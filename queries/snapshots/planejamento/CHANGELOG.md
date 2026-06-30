# Changelog - snapshots/planejamento

## [1.0.2] - 2026-06-11

### Alterado

- Altera `snapshot_viagem_planejada_planejamento` para utilizar `viagem_planejada_planejamento_dia` como fonte e `concat(data, '-', id_viagem)` como `unique_key` (https://github.com/RJ-SMTR/pipelines_v3/pull/101)

## [1.0.1] - 2026-02-10

### Adicionado

- Cria snapshot `snapshot_tecnologia_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1161)

## [1.0.0] - 2025-07-03

### Adicionado

- Cria snapshot `snapshot_tecnologia_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)