# Changelog - transito

## [1.0.4] - 2025-04-14

## Adicionado

- Adicionada a view `autuacao_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/547)

## [1.0.3] - 2025-04-09

## Adicionado

- Adicionada a view `autuacao_serpro` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/526)
- Adicionada a tabela `aux_autuacao_id` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/526)

## Alterado

- Alterado a tabela `autuacao` incluindo join da view `autuacao_serpro` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/526)

## [1.0.2] - 2024-09-06

## Adicionado

- Adicionada a tabela `receita_autuacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/194)

## [1.0.1] - 2024-09-04

### Corrigido

- Corrige colunas `status_infracao` e `descricao_situacao_autuacao` e o filtro incremental da tabela autuacao (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/186/)

## [1.0.0] - 2024-08-03

### Adicionado

- Adicionada a tabela `autuacao` e a view `autuacao_citran` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/184)