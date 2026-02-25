# Changelog - bilhetagem_interno

## [1.2.1] - 2026-02-25

### Adicionado

- Migra modelos `staging_lancamento.sql` e `extrato_cliente_cartao.sql` para o repositório v3

## [1.2.0] - 2026-01-26

### Adicionado

- Adiciona obrigatoriedade no filtro de partição no modelo da tabela `extrato_cliente_cartao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1186)

## [1.1.9] - 2025-12-10

### Adicionado

- Adiciona coluna `numero_matricula` nos modelos `aux_gratuidade_info.sql` e `transacao_gratuidade_estudante_municipal.sql`
- Adiciona coluna `id_unico_lancamento` no modelo `extrato_cliente_cartao.sql`

### Alterado

- Altera deduplicação no modelo `extrato_cliente_cartao.sql`

## [1.1.8] - 2025-11-03

### Adicionado

- Adiciona teste de sincronia dos dados no modelo `transacao_gratuidade_estudante_municipal.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1011)

## [1.1.7] - 2025-10-20

### Alterado

- Altera o modelo de view para modelo incremental `transacao_gratuidade_estudante_municipal.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/958)

## [1.1.6] - 2025-10-14

### Alterado

- Altera deduplicação da tabela `lancamento` para coluna `id_lancamento` nula no modelo `extrato_cliente_cartao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/949)

## [1.1.5] - 2025-10-14

### Alterado

- Altera deduplicação da tabela `lancamento` para coluna `id_lancamento` nula no modelo `extrato_cliente_cartao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/948)

## [1.1.4] - 2025-09-22

### Adicionado

- Cria modelo `extrato_cliente_cartao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)

### Alterado

- Altera o tratamento da coluna cd_cliente do modelo `staging_lancamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/855)

## [1.1.3] - 2025-09-16

### Adicionado

- Adiciona a coluna `id_cre_escola` no modelo `transacao_gratuidade_estudante_municipal.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875)


## [1.1.2] - 2025-08-27

### Alterado

- Altera colunas do modelo `transacao_gratuidade_estudante_municipal.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/812)

## [1.1.0] - 2025-08-21

### Adicionado

- Cria modelo `transacao_gratuidade_estudante_municipal.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/804)

## [1.0.1] - 2025-08-11

### Adicionado

- Cria modelo `data_ordem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/763)

## [1.0.0] - 2025-07-29

### Adicionado

- Cria modelos `staging_lancamento` e `recarga_jae.sql`