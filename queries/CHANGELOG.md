# Changelog - queries

## [1.1.8] - 2026-07-08

### Adicionado

- Cria selectors `riorotativo_diario` (materialização dos modelos), `riorotativo_snapshot` (snapshot, executado após o diário) e `riorotativo_historico` (backfill/reprocessamento por período, sem modelos de estado atual) (https://github.com/RJ-SMTR/pipelines_v3/pull/355)

## [1.1.7] - 2026-06-26

### Corrigido

- Corrige nomes dos modelos `autuacao_citran` e `autuacao_serpro` para `view_autuacao_citran` e `view_autuacao_serpro` no selector `transito_autuacao` (https://github.com/RJ-SMTR/pipelines_v3/pull/317)

## [1.1.6] - 2026-06-24

### Alterado

- Amplia para 3h a tolerância dos testes hourly de freshness de `view_gps_onibus` e `gps_validador` (https://github.com/RJ-SMTR/pipelines_v3/pull/310)

## [1.1.5] - 2026-06-02

### Adicionado

- Cria view `view_gps_onibus` unindo os dados de GPS dos fornecedores `sppo`, `zirix`, `cittati` e `conecta` (https://github.com/RJ-SMTR/pipelines_v3/pull/224)
- Adiciona teste `dbt_expectations.expect_grouped_row_values_to_have_recent_data` na coluna `datetime_gps` da view `view_gps_onibus`, com tags `freshness` e `hourly`, verificando freshness por fornecedor (https://github.com/RJ-SMTR/pipelines_v3/pull/224)

### Alterado

- Altera tags do teste de freshness `dbt_expectations.expect_row_values_to_have_recent_data__datetime_captura__gps_validador` de `freshness_hourly` para `freshness` e `hourly` (https://github.com/RJ-SMTR/pipelines_v3/pull/224)

## [1.1.4] - 2026-05-26

### Adicionado

- Cria exposure `sistema_ia_processamento_documento_transito` referente ao acesso da tabela `transito.autuacao` pela solução de IA da I3 Soluções Ltda. (Contrato SMTR nº 10/2026), conforme TED_007-26_DTDI-SUBG — Processo SEI nº 000300.013844/2026-83 (https://github.com/RJ-SMTR/pipelines_v3/pull/213)

## [1.1.3] - 2026-05-06

### Adicionado

- Adiciona a tabela `gps_15_minutos_onibus_sppo` ao arquivo `sources.yml` (https://github.com/RJ-SMTR/pipelines_v3/pull/162)

## [1.1.2] - 2026-04-28

### Alterado

- Altera o tempo de timeout da key `job_execution_timeout_seconds` do `profiles.yml` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/132)

## [1.1.1] - 2025-09-23

### Alterado

- Altera selector `cadastro` adicionando os modelos de view `consorcio_modo` e `modos` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/886)

## [1.1.0] - 2025-09-15

### Alterado

- Altera selector `apuracao_subsidio_v8` adicionando o modelo `percentual_operacao_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)

## [1.0.9] - 2025-09-02

### Adicionado

- Cria selector `snapshot_temperatura` e `monitoramento_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)

### Alterado

- Altera selector `snapshot_veiculo` removendo os snapshots `snapshot_veiculo_regularidade_temperatura_dia`, `snapshot_temperatura_inmet` e `snapshot_viagem_regularidade_temperatura`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)
- Altera selector `snapshot_viagem` movendo os snapshots `snapshot_viagens_remuneradas`, `snapshot_viagem_transacao` para o `snapshot_subsidio`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/818)
- Altera selector `apuracao_subsidio_v9` substituindo `models/financeiro` por `subsidio_faixa_servico_dia_tipo_viagem`, `subsidio_penalidade_servico_faixa` e `subsidio_sumario_servico_dia_pagamento`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/818)

## [1.0.8] - 2025-08-19

### Alterado

- Altera selector `snapshot_veiculo` adicionando os snapshots `snapshot_veiculo_regularidade_temperatura_dia`, `snapshot_temperatura_inmet` e `snapshot_viagem_regularidade_temperatura`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/796)

### Adicionado

- Adiciona filtro de data para os modelos de `bilhetagem` e `br_rj_riodejaneiro_bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783)

## [1.0.7] - 2025-07-31

### Alterado

- Altera selector `apuracao_subsidio_v9` excluindo o modelo `aux_viagem_temperatura` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Altera selector `monitoramento_veiculo` adicionando os modelos `aux_viagem_temperatura`, `temperatura_inmet`, `aux_veiculo_falha_ar_condicionado`, `veiculo_regularidade_temperatura_dia` e `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

## [1.0.6] - 2025-07-09

### Alterado

- Altera selector `apuracao_subsidio_v9` para executar todos os modelos do dataset `subsidio` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

## [1.0.5] - 2025-06-27

### Alterado

- Altera selector `apuracao_subsidio_v9` adicionando o modelo `percentual_operacao_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)

## [1.0.4] - 2025-05-20

### Adicionado

- Adiciona selector `gps` e `gps_15_minutos` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/297)

## [1.0.3] - 2025-05-13

### Alterado

- Altera selector `planejamento_diario` adicionando o modelo `servico_planejado_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/575)

## [1.0.2] - 2025-04-08

### Alterado

- Altera selector `apuracao_subsidio_v9` adicionando o modelo `staging_tecnologia_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/530)

## [1.0.1] - 2025-01-28

### Alterado

- Altera selector `apuracao_subsidio_v9` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

## [1.0.0] - 2024-10-29

### Adicionado

- Adiciona package: `dbt-labs/dbt_utils` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/288)
