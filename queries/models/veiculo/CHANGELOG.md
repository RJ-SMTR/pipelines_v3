# Changelog - veiculo

## [2.1.4] - 2026-03-02

### Alterado

- Formatados os modelos `sppo_registro_agente_verao.sql` e `sppo_registro_agente_verao_staging.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/510)

## [2.1.5] - 2025-07-08

### Corrigido

- Corrigido a coluna `status` no modelo `sppo_veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/677)

## [2.1.4] - 2025-07-08

### Corrigido

- Corrigida a coluna `ano_ultima_vistoria` no modelo `licenciamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/676)

## [2.1.3] - 2025-07-02

### Corrigido

- Corrigido o teste `dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart` no modelo `sppo_veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/656)

## [2.1.2] - 2025-07-01

### Adicionado

- Criado o modelo `sppo_vistoria_tr_subtt_cmo_recurso_SMTR202404004977_staging.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/124)

### Alterado

- Alterado o modelo `aux_sppo_licenciamento_vistoria_atualizada` para incluir o modelo `sppo_vistoria_tr_subtt_cmo_recurso_SMTR202404004977_staging.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/124)

## [2.1.1] - 2025-06-30

### Adicionado

- Adicionada a coluna `datetime_infracao` no modelo `infracao_staging.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/637)

## [2.1.0] - 2025-06-25

### Alterado

- Limita a data máxima para materialização de dados de veículo nos modelos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632):
  - `infracao.sql`
  - `licenciamento.sql`
  - `sppo_veiculo_dia.sql`

- Altera referência das views de staging nos modelos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632):
  - `infracao.sql`
  - `licenciamento.sql`
  - `infracao_data_versao_efetiva.sql`
  - `licenciamento_data_versao_efetiva.sql`

- Move modelo `infracao_staging.sql` do dataset `veiculo_staging` para `monitoramento_staging` e renomeia para `staging_infracao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

- Move modelo `licenciamento_stu_staging.sql` do dataset `veiculo_staging` para `cadastro_staging` e renomeia para `staging_licenciamento_stu.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [2.0.8] - 2025-06-12

### Adicionado

- Adicionada exceção no modelo `licenciamento_data_versao_efetiva.sql` para utilizar a data_versao `2025-03-22` no período de `2023-10-01`a `2024-01-31` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/590)


## [2.0.7] - 2025-05-27

### Adicionado

- Adiciona coluna `ultima_situacao` no modelo `licenciamento_stu_staging.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/594)

## [2.0.6] - 2025-05-09

### Alterado
- Alterado o modelo `sppo_veiculo_dia.sql` para remover os status `Autuado por segurança` e `Autuado por limpeza/equipamento` a partir e 2025-04-01 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/572)

## [2.0.5] - 2025-04-08

### Adicionado
- Adicionados os modelos `infracao_data_versao_efetiva.sql` e `licenciamento_data_versao_efetiva.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/434)

### Alterado
- Refatorados os modelos `licenciamento.sql`, `infracao.sql` e `sppo_veiculo_dia.sql` para materializar períodos inteiros de uma vez (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/434)

- Substituídas as macros `get_license_date.sql` e `get_violation_date.sql` por `get_version_dates.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/434)

## [2.0.4] - 2025-02-24

### Alterado
- Alterado o modelo `licenciamento.sql` para corrigir os dados modificados da coluna `tipo_veiculo` conforme Processo.Rio MTR-CAP-2025/01125 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/457)

## [2.0.3] - 2025-02-06

### Adicionado
- Adicionados testes no modelo `infracao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

## [2.0.2] - 2025-01-23

### Alterado
- Encerrada regra de transição com dados temporários da CGLF no modelo `licenciamento.sql` e reformatação deste (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/395)

### Corrigido
- Corrigidos os tipos das colunas `tecnologia`, `placa`, `data_licenciamento` e `data_infracao` na materialização antes de `2025-01-01` no modelo `sppo_veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/395)

## [2.0.1] - 2025-01-21

### Corrigido
- Corrigido os sources dos modelos `infracao.sql` e `licenciamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/391)

## [2.0.0] - 2025-01-16

### Adicionado
- Criado os modelos `infracao.sql` e `licenciamento.sql` para guardar os dados completos de infração e licenciamento do STU (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/126)

### Alterado
- Altera tipo dos modelos `sppo_infracao.sql` e `sppo_licenciamento.sql` para `ephemeral` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/126)
- Adiciona colunas `tecnologia`, `placa`, `data_licenciamento`, `data_infracao` e `datetime_ultima_atualizacao` no modelo `sppo_veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/126)

## [1.1.4] - 2024-11-13

#### Adicionado

- Adiciona testes do subsidio para `sppo_veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/256)

## [1.1.3] - 2024-10-25

#### Alterado

- Altera lógica do filtro do modelo `sppo_aux_registros_realocacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/287)

#### Adicionado

- Adiciona testes do DBT no schema (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/287)

## [1.1.2] - 2024-04-25

### Adicionado

- Criada macro `get_license_date.sql` para retornar relacionamento entre `run_date` e data de licenciamento dos modelos `sppo_licenciamento_stu_staging.sql`, `sppo_licenciamento.sql` e `sppo_veiculo_dia.sql`. Nesta macro, serão admitidas apenas versões do STU igual ou após 2024-04-09 a partir de abril/24 devido à falha de atualização na fonte da dados (SIURB) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/280)

### Corrigido

- Altera lógica de particionamento nos modelos `sppo_licenciamento_stu_staging.sql`, `sppo_licenciamento.sql`, `sppo_infracao_staging.sql` e `sppo_infracao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/280)

## [1.1.1] - 2024-04-16

### Corrigido

- Cria lógica de deduplicação na tabela `sppo_registro_agente_verao` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/271)

## [1.1.0] - 2024-04-15

### Alterado

- Reorganizar modelos intermediários de licenciamento em staging (https://github.com/prefeitura-rio/queries-rj-smtr/pull/255)
- Atualiza schema para refletir as alterações (https://github.com/prefeitura-rio/queries-rj-smtr/pull/255)

## [1.0.2] - 2024-04-12

### Alterado

- Fixa versão do STU em `2024-04-09` para mar/Q2 devido à falha de atualização na fonte da dados (SIURB) nos modelos `sppo_licenciamento.sql` e `sppo_veiculo_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/264)

## [1.0.1] - 2024-04-05

### Alterado
- Altera a localização da verificação de validade da vistoria de `sppo_licenciamento` para `sppo_veiculo_dia` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/252)
- Adiciona coluna `data_inicio_veiculo` na tabela `sppo_licenciamento` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/252)

## [1.0.0] - 2024-03-30

### Adicionado

- Nova tabela de atualização do ano de vistoria dos ônibus a partir dos
  dados enviados pela
  Coordenadoria Geral de Fiscalização e Licenciamento (CGFL) em
  2024-03-20:
  `aux_sppo_licenciamento_vistoria_atualizada.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
  - A tabela contém todos os veículos vistoriados em 2023 e
  2024 (incluindo agendados mas ainda pendentes)

### Alterado

- Adiciona coluna `data_inicio_vinculo` na tabela `sppo_licenciamento` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Adiciona indicador de vistoria ativa do veículo na tabela
  `sppo_veiculo_dia` para versão 5.0.0 do subsídio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Atualiza documentação de tabelas e colunas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Alterações feitas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/229 e https://github.com/prefeitura-rio/queries-rj-smtr/pull/236 corrigidas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/239

### Corrigido

- Corrige versão dos dados de licenciamento do STU a partir de 01/03/24
  na tabela `sppo_licenciamento` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
  - Versão dos dados foi fixada em 25/03 (última extração recebida) devido uma
    falha de atualização da fonte de dados (SIURB) aberta em 22/01 que
    ainda não foi resolvida

