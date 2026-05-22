# Changelog - dashboard_subsidio_sppo_v2

## [1.1.2] - 2025-10-30

### Adicionado

- Cria modelo `sumario_servico_dia_pagamento_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1016)

## [1.1.1] - 2025-08-07

### Alterado

- Alterado o modelo `sumario_faixa_servico_dia_pagamento` para utilizar o versionamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

## [1.1.0] - 2025-06-25

### Alterado

- Altera lógica do modelo `sumario_faixa_servico_dia_pagamento` para utilizar o macro `generate_km_columns` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/639)

## [1.0.9] - 2025-06-24

### Alterado

- Altera referência da tabela `subsidio_faixa_servico_dia` para `percentual_operacao_faixa_horaria` nos modelos `sumario_faixa_servico_dia`, `sumario_faixa_servico_dia_pagamento` e `sumario_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)

## [1.0.8] - 2025-05-14

### Alterado

- Alterado o calculo da coluna `valor_judicial` para não somar o `valor_penalidade` a partir e `2025-04-01` no modelo `sumario_faixa_servico_dia_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/580)

## [1.0.7] - 2025-05-09

### Corrigido

- Corrigido o modelo `sumario_faixa_servico_dia_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/572)

## [1.0.6] - 2025-02-17

### Corrigido

- Corrigido teste `km_planejada_faixa` maior que zero do modelo `sumario_faixa_servico_dia_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/433)

## [1.0.5] - 2025-02-04

### Adicionado

- Adicionados os testes do modelo `sumario_faixa_servico_dia_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

### Corrigido

- Corrigidos os testes do modelo `sumario_servico_dia_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/401)

## [1.0.4] - 2025-01-28

### Removido

- Remove alterações no modelo `sumario_faixa_servico_dia` pelo PR #390 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

### Alterado

- Altera lógica do modelo `sumario_faixa_servico_dia` para não materializar quando a data for maior que `DATA_SUBSIDIO_V14_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

### Adicionado

- Cria modelo `sumario_faixa_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

## [1.0.3] - 2025-01-27

### Adicionado

- Adiciona as colunas `km_apurada_licenciado_sem_ar_n_autuado_mini`, `km_apurada_licenciado_com_ar_n_autuado_mini`, `km_apurada_licenciado_sem_ar_n_autuado_midi`, `km_apurada_licenciado_com_ar_n_autuado_midi`, `km_apurada_licenciado_sem_ar_n_autuado_basico`, `km_apurada_licenciado_com_ar_n_autuado_basico`, `km_apurada_licenciado_sem_ar_n_autuado_padron`, `km_apurada_licenciado_com_ar_n_autuado_padron`, `km_apurada_total_licenciado_sem_ar_n_autuado`, `km_apurada_total_licenciado_com_ar_n_autuado`, `valor_a_pagar`, `valor_glosado_tecnologia`, `valor_total_glosado`, `valor_total_apurado`, `valor_judicial`, `valor_penalidade` no modelo `sumario_faixa_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

### Alterado

- Altera nome da coluna `km_apurada_licenciado_sem_ar_n_autuado` para `km_apurada_total_licenciado_sem_ar_n_autuado`, `km_apurada_licenciado_com_ar_n_autuado` para `km_apurada_total_licenciado_com_ar_n_autuado` e `valor_glosado` para `valor_total_glosado` no modelo `sumario_faixa_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)
- Altera lógica do modelo `sumario_servico_dia_pagamento` para não materializar quando a data for maior que `DATA_SUBSIDIO_V14_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

### Removido

- Remove a coluna `valor_apurado` no modelo `sumario_faixa_servico_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [1.0.2] - 2024-10-02

### Adicionado

- Adiciona testes do subsidio para `sumario_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/256)

## [1.0.1] - 2024-09-20

### Alterado

- Adiciona coalesce no desvp_pof (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/239)

## [1.0.0] - 2024-08-29

### Adicionado

- Cria modelos `sumario_faixa_servico_dia` e `sumario_servico_dia_pagamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)