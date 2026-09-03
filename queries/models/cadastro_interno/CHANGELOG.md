# Changelog - cadastro_interno

## [1.2.2] - 2026-09-03

### Adicionado

- Renomeia modelo `aux_operadora_jae_cliente_historico.sql` para `aux_cliente_jae_historico.sql` e move para o `cadastro_interno` ()

### Removido

- Remove filtro de `id_cliente` do modelo `aux_cliente_jae_historico.sql` ()

## [1.2.1] - 2025-09-17

### Alterado

- Altera número de partições dos modelos `endereco_cliente_cpf_jae.sql` e `cliente_cpf_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/878)

## [1.2.1] - 2025-09-11

### Adicionado

- Adiciona coluna `cpf` no modelo `endereco_cliente_cpf_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/871)

### Alterado

- Renomeia coluna `documento` para `cpf` no modelo `cliente_cpf_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/871)

### Removido

- Remove coluna `nome_social` no modelo `cliente_cpf_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/871)

### Corrigido

- Muda strings vazias para null na coluna `numero` do modelo `endereco_cliente_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/871)

## [1.2.0] - 2025-09-08

### Adicionado

- Cria modelo `endereco_cliente_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/854)

## [1.0.0] - 2025-08-27

### Adicionado

- Cria modelo `cliente_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/812)
