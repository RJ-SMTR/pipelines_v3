# Changelog - riorotativo

## [1.2.6] - 2026-10-15

### Alterado

- Descomenta tabela do bloqueios no modelo `guardador_veiculo_riorotativo_historico.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/768)

### Removido

- Remove `enabled=False` dos modelo `staging_lista_bloqueio_riorotativo.sql`, `staging_perfil_funcionamento_excecao_riorotativo.sql` e `perfil_funcionamento_riorotativo_historico.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/768)

## [1.2.5] - 2026-10-14

### Adicionado

- Adiciona a descrição dos testes no `config.description` do schema, usada nas notificações dos testes (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.2.4] - 2026-10-14

### Adicionado

- Cria os modelos `staging_guardador_veiculo_05019730000158_riorotativo.sql` e `staging_guardador_veiculo_34152025000122_riorotativo.sql`, um por entidade credenciadora, sem deduplicação (https://github.com/RJ-SMTR/pipelines_v3/pull/748)
- Adiciona o teste `not_null` em `identificacao` e a regra de contrato `duplicateValues` (unicidade por captura) nos stagings de guardadores de veículo (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

### Alterado

- Move para `guardador_veiculo_riorotativo_historico.sql` a seleção da última captura de cada dia, com `union` dos stagings por entidade (https://github.com/RJ-SMTR/pipelines_v3/pull/748)
- Atualiza `entidade_credenciadora_riorotativo_historico.sql` para os stagings por entidade (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

### Removido

- Remove o modelo `staging_guardador_veiculo_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/748)
- Remove as `policy_tags` dos stagings, que são views e não aplicam policy tags (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

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
