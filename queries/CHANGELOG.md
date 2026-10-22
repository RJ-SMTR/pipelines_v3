# Changelog - queries

## [1.2.2] - 2026-10-22

### Adicionado

- Cria o teste genérico `sequential_order`, que aponta registros cuja coluna sequencial é maior que a de registros com valor posterior na coluna de ordenação (https://github.com/RJ-SMTR/pipelines_v3/pull/790)

## [1.2.1] - 2026-10-15

### Corrigido

- Corrige a recomposição incremental de `gps_aux_filtrada` (v1 e v2), `gps_tratamento` e `gps_tratamento_v2` para preservar os registros existentes em todas as datas da janela de processamento, inclusive na virada do dia ou quando não há novas capturas em uma das datas (https://github.com/RJ-SMTR/pipelines_v3/pull/770).
- Preserva também a partição de captura do dia seguinte na filtrada quando `date_range_end` termina em `23:59:59`, acompanhando a expansão de um segundo já usada na leitura da origem (https://github.com/RJ-SMTR/pipelines_v3/pull/770).
- Corrige `gps_staging_registros_v2` para preservar `data` e `hora` das partições de captura e adiciona à versão 2 de `gps_aux_filtrada` os filtros de captura e de `datetime_gps` usados na versão 1, reduzindo a leitura do histórico da Maxtrack no BigQuery (https://github.com/RJ-SMTR/pipelines_v3/pull/770).
- Corrige `gps_tratamento_v2` para derivar `data` e `hora` de `datetime_gps` na tabela final, como na versão 1 (https://github.com/RJ-SMTR/pipelines_v3/pull/770).

### Removido

- Remove a macro `get_date_hour_partitions`, sem consumidores após a correção da recomposição das partições de GPS (https://github.com/RJ-SMTR/pipelines_v3/pull/770).

## [1.2.0] - 2026-10-14

### Adicionado

- Cria macros de GPS em `macros/gps/` (`gps_staging_registros`, `gps_staging_registros_v2`, `gps_staging_realocacao`, `gps_aux_realocacao`, `gps_aux_filtrada`, `gps_aux_velocidade`, `gps_aux_parada`, `gps_aux_trajeto_correto`, `gps_tratamento`, `gps_tratamento_v2` e `gps_15_minutos_tratamento`) com o SQL comum aos modelos de GPS por fonte (https://github.com/RJ-SMTR/pipelines_v3/pull/757)
- Cria macro `get_date_hour_partitions` para listar as partições de `data` de uma relação no intervalo `date_range_start`/`date_range_end` (https://github.com/RJ-SMTR/pipelines_v3/pull/757)
- Cria selectors `gps_conecta`, `gps_zirix`, `gps_cittati`, `gps_sppo`, `gps_sonda`, `gps_15_minutos_conecta`, `gps_15_minutos_zirix`, `gps_15_minutos_cittati`, `gps_15_minutos_sppo` e `gps_v2_maxtrack` (https://github.com/RJ-SMTR/pipelines_v3/pull/757)

### Alterado

- Altera os testes `check_gps_capture` e `check_gps_capture_v2` para usar o próprio modelo testado e os testes `check_gps_treatment` e `check_gps_treatment_v2` para receber `registros` e `aux_filtrada` como argumentos (https://github.com/RJ-SMTR/pipelines_v3/pull/757)
- Altera o selector `viagem_inferida` para listar explicitamente `aux_monitoramento_registros_status_trajeto` e `view_gps_onibus`, removendo `parents: true` e as exclusões de `gtfs` e `planejamento` (https://github.com/RJ-SMTR/pipelines_v3/pull/757)

### Removido

- Remove os selectors `gps`, `gps_15_minutos` e `gps_v2` (https://github.com/RJ-SMTR/pipelines_v3/pull/757)
- Remove as variáveis `15_minutos`, `modo_gps` e `fonte_gps` do `dbt_project.yml` (https://github.com/RJ-SMTR/pipelines_v3/pull/757)

## [1.1.12] - 2026-10-14

### Adicionado

- Adiciona a descrição dos testes singulares em `tests/` no `config.description`, usada nas notificações dos testes (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.1.11] - 2026-10-14

### Adicionado

- Sobrescreve os testes `not_null`, `unique` e `accepted_values` com descrição padrão em português no `config.description` (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

### Alterado

- Atualiza o selector `riorotativo_auxiliar` com os stagings de guardadores de veículo por entidade credenciadora (https://github.com/RJ-SMTR/pipelines_v3/pull/748)

## [1.1.10] - 2026-07-28

### Adicionado

- Cria selector `viagem_inferida` (https://github.com/RJ-SMTR/pipelines_v3/pull/444)

## [1.1.9] - 2026-07-15

### Adicionado

- Cria selector `snapshot_riorotativo` (https://github.com/RJ-SMTR/pipelines_v3/pull/378)

## [1.1.8] - 2026-07-08

### Adicionado

- Cria macro `is_current_state_enabled` para desligar modelos de estado atual em backfills com janela antiga (https://github.com/RJ-SMTR/pipelines_v3/pull/355)
- Cria selector `riorotativo_diario` (materialização dos modelos; backfills com janela antiga executam apenas stagings e históricos via `is_current_state_enabled`) (https://github.com/RJ-SMTR/pipelines_v3/pull/355)

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
