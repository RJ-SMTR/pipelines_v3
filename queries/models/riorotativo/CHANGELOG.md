# Changelog - riorotativo

## [1.2.3] - 2026-10-05

### Adicionado

- Adiciona metadados `datacontract_cli` (chave primária e regras de qualidade) no modelo `staging_guardador_veiculo_riorotativo.sql` para geração do contrato de dados (https://github.com/RJ-SMTR/pipelines_v3/pull/694)
- Adiciona teste de formato numérico para `numero_identificacao` no modelo `guardador_veiculo_riorotativo_historico.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/694)

### Alterado

- Renomeia as colunas `documento`, `numero_identificacao` e `datetime_captura` para `cpf`, `identificacao` e `timestamp_captura` no modelo `staging_guardador_veiculo_riorotativo.sql`, deduplicando por `data`, `cnpj` e `cpf` (https://github.com/RJ-SMTR/pipelines_v3/pull/694)
- Mapeia as colunas do staging para os nomes anteriores no modelo `guardador_veiculo_riorotativo_historico.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/694)

### Removido

- Remove a coluna `tipo_documento` do modelo `staging_guardador_veiculo_riorotativo.sql`, mantida como constante `CPF` no histórico (https://github.com/RJ-SMTR/pipelines_v3/pull/694)

## [1.2.2] - 2026-09-01

### Adicionado

- Cria modelo `ativacao_hora_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/561)
- Adiciona coluna `datetime_inclusao` no modelo `guardador_veiculo_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/561)

## [1.2.1] - 2026-08-27

### Adicionado

- Adiciona colunas `id_veiculo_cliente`, `latitude`, `longitude` e `area_codigo` no modelo `staging_movimento_estacionamento_veiculo_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/578)

### Alterado

- Altera fonte das colunas `id_veiculo_cliente`, `latitude`, `longitude` e `area_codigo` no modelo `ativacao_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/578)

### Removido

- Remove colunas `data_inicio_vigencia_area` e `data_fim_vigencia_area` do modelo `ativacao_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/578)

## [1.2.0] - 2026-07-30

### Adicionado

- Cria modelos (https://github.com/RJ-SMTR/pipelines_v3/pull/458):
  - `aux_verificacao_particao_captura_riorotativo.sql`
  - `staging_fiscalizacao_veiculo_riorotativo.sql`
  - `staging_veiculo_cliente_riorotativo.sql`
  - `staging_veiculo_riorotativo.sql`
  - `aux_ativacao_particao_captura_riorotativo.sql`
  - `staging_estacionamento_veiculo_riorotativo.sql`
  - `staging_movimento_estacionamento_veiculo_riorotativo.sql`
  - `ativacao_riorotativo.sql`

## [1.1.0] - 2026-07-16

### Adicionado

- Adiciona `numero_identificacao` aos modelos de guardadores de veículo, normalizado
  com quatro dígitos, e teste de unicidade (https://github.com/RJ-SMTR/pipelines_v3/pull/384)

## [1.0.1] - 2026-07-15

### Alterado

- Adiciona colunas de controle (`versao`, `datetime_ultima_atualizacao`, `id_execucao_dbt`) em (https://github.com/RJ-SMTR/pipelines_v3/pull/378):
  - `guardador_veiculo_riorotativo.sql`
  - `area_estacionamento_riorotativo.sql`
  - `perfil_funcionamento_riorotativo.sql`
  - `agente_verificacao_riorotativo.sql`

## [1.0.0] - 2026-07-08

### Adicionado

- Cria modelos (https://github.com/RJ-SMTR/pipelines_v3/pull/355):
  - `staging_guardador_veiculo_riorotativo.sql`
  - `staging_agente_verificacao_riorotativo.sql`
  - `staging_area_estacionamento_riorotativo.sql`
  - `staging_lista_bloqueio_riorotativo.sql`
  - `staging_perfil_funcionamento_riorotativo.sql`
  - `staging_perfil_funcionamento_excecao_riorotativo.sql`
  - `guardador_veiculo_riorotativo.sql`
  - `guardador_veiculo_riorotativo_historico.sql`
  - `agente_verificacao_riorotativo.sql`
  - `agente_verificacao_riorotativo_historico.sql`
  - `area_estacionamento_riorotativo.sql`
  - `perfil_funcionamento_riorotativo.sql`
  - `perfil_funcionamento_riorotativo_historico.sql`
  - `entidade_credenciadora_riorotativo_historico.sql`
