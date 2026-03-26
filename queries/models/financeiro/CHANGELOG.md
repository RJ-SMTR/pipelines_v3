# Changelog - financeiro

## [1.2.0] - 2025-10-09

### Adicionado

- Cria modelos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/934):
  - `staging_detalhe_a_cct.sql`
  - `staging_ordem_pagamento_agrupado_cct.sql`
  - `staging_ordem_pagamento_agrupado_historico_cct.sql`
  - `staging_ordem_pagamento_cct.sql`
  - `staging_user_cct.sql`
  - `dicionario_financeiro.sql`

## [1.1.1] - 2025-09-24

### Alterado

- Move modelos das tabelas do subsidio de `rj-smtr.financeiro` para `rj-smtr.financeiro_interno` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/893)

## [1.1.0] - 2025-09-01

### Alterado

- Move modelos do dataset `br_rj_riodejaneiro_bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [1.0.5] - 2025-08-07

### Alterado

- Alterados os modelos `subsidio_faixa_servico_dia_tipo_viagem` e `subsidio_penalidade_servico_faixa` para utilizar o versionamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

## [1.0.4] - 2025-06-25

### Alterado

- Altera fonte dos dados de veículo para `aux_veiculo_dia_consolidada` no modelo `subsidio_faixa_servico_dia_tipo_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [1.0.3] - 2025-06-24

### Alterado

- Altera referência da tabela `subsidio_faixa_servico_dia` para `percentual_operacao_faixa_horaria` nos modelos `subsidio_faixa_servico_dia_tipo_viagem`, `subsidio_penalidade_servico_faixa` e `subsidio_sumario_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)

## [1.0.2] - 2025-01-27

### Adicionado

- Cria modelo `subsidio_penalidade_servico_faixa` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)
- Adiciona as colunas `tecnologia_apurada`, `tecnologia_remunerada` e `valor_glosado_tecnologia` no modelo `subsidio_faixa_servico_dia_tipo_viagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

### Alterado

- Altera lógica do modelo `subsidio_sumario_servico_dia_pagamento` para não materializar quando a data for maior que `DATA_SUBSIDIO_V14_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

### Removido

- Remove modelo `subsidio_penalidade_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [1.0.1] - 2024-09-20

### Alterado

- Altera `km_subsidiada_faixa` para considerar kms com indicador_viagem_dentro_limite = TRUE, pof >= 80 e subsidio_km > 0 no modelo `subsidio_faixa_servico_dia_tipo_viagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/239)

## [1.0.0] - 2024-08-29

### Adicionado

- Cria modelos `subsidio_penalidade_servico_dia`, `subsidio_faixa_servico_dia_tipo_viagem` e `subsidio_sumario_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)